from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

import httpx

from hermes.config import CoordinatorSettings, WorkerSettings
from hermes.coordinator import create_app
from hermes.storage import SQLiteStore
from hermes.worker import WorkerAPI, WorkerRuntime


class WorkerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        coordinator_settings = CoordinatorSettings(
            database_path=database,
            director_api_key="director-test-key",
            worker_shared_secret="worker-test-secret",
            task_lease_seconds=30,
        )
        self.store = SQLiteStore(database)
        self.store.initialize()
        app = create_app(coordinator_settings, self.store)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )
        self.api = WorkerAPI(
            "http://test", "mock-worker", "worker-test-secret", self.client
        )
        self.worker_settings = WorkerSettings(
            coordinator_url="http://test",
            worker_id="mock-worker",
            worker_name="Mock Worker",
            shared_secret="worker-test-secret",
            default_agent="codex",
            command=[
                sys.executable,
                "-c",
                "import sys; print('mock-hermes: ' + sys.argv[1])",
                "{prompt}",
            ],
            allowed_workdir=self.temporary_directory.name,
            concurrency=1,
            task_timeout_seconds=10,
            heartbeat_interval_seconds=1,
            poll_interval_seconds=0,
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.temporary_directory.cleanup()

    async def test_worker_executes_and_reports_result(self) -> None:
        await self.api.register("Mock Worker", 1)
        created = self.store.create_task(
            {"prompt": "integration hello", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        task_id = created["id"]
        task = await self.api.claim()
        self.assertEqual(task_id, task["id"])
        runtime = WorkerRuntime(self.worker_settings, self.api)
        await runtime.run_task(task)
        result = self.store.get_task(task_id)
        self.assertEqual("succeeded", result["status"])
        self.assertEqual("mock-hermes: integration hello\n", result["result"])

    async def test_runtime_registers_structured_command_worker(self) -> None:
        runtime = WorkerRuntime(self.worker_settings, self.api)

        await runtime.run(once=True)

        worker = self.store.list_workers(stale_seconds=45)[0]
        self.assertEqual("command", worker["worker_kind"])
        self.assertEqual("codex", worker["default_agent"])
        self.assertEqual("codex", worker["routes"][0]["default_agent"])

    async def test_workdir_escape_and_bypass_are_rejected(self) -> None:
        runtime = WorkerRuntime(self.worker_settings, self.api)
        with self.assertRaises(ValueError):
            runtime.resolve_workdir("../outside")
        dangerous = WorkerSettings(
            worker_id="mock-worker",
            worker_name="Mock Worker",
            shared_secret="worker-test-secret",
            command=["hermes", "--dangerously-bypass-approvals", "{prompt}"],
            allowed_workdir=self.temporary_directory.name,
        )
        with self.assertRaises(ValueError):
            WorkerRuntime(dangerous, self.api)

    async def test_output_reader_is_memory_bounded(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(b"x" * 2_100_000)
        reader.feed_eof()
        output = await WorkerRuntime._read_limited(reader)
        self.assertLess(len(output), 2_100_000)
        self.assertIn(b"output truncated", output)

    async def test_agent_command_and_execution_prompt_are_selected(self) -> None:
        settings = WorkerSettings(
            worker_id="agent-worker",
            worker_name="Agent Worker",
            shared_secret="worker-test-secret",
            default_agent="codex",
            agent_commands={
                "codex": [
                    sys.executable,
                    "-c",
                    "import sys; print('codex: ' + sys.argv[1])",
                    "{prompt}",
                ],
                "claude": [
                    sys.executable,
                    "-c",
                    "import sys; print('claude: ' + sys.argv[1])",
                    "{prompt}",
                ],
            },
            allowed_workdir=self.temporary_directory.name,
        )
        runtime = WorkerRuntime(settings, self.api)
        self.assertEqual(
            [
                sys.executable,
                "-c",
                "import sys; print('claude: ' + sys.argv[1])",
                "plan",
            ],
            runtime.command_for("plan", "claude"),
        )
        self.assertEqual(["agent:claude", "agent:codex"], runtime.capabilities())
        with self.assertRaises(ValueError):
            runtime.command_for("x", "unknown")

        await self.api.register("Mock Worker", 1)
        created = self.store.create_task(
            {"prompt": "original", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        task_id = created["id"]
        task = await self.api.claim()
        task["execution_agent"] = "claude"
        task["execution_prompt"] = "specialized"
        await runtime.run_task(task)
        result = self.store.get_task(task_id)
        self.assertEqual("succeeded", result["status"])
        self.assertEqual("claude: specialized\n", result["result"])


if __name__ == "__main__":
    unittest.main()
