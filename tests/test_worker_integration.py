from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from hermes.config import CoordinatorSettings, UnifiedWorkerSettings, WorkerSettings
from hermes.coordinator import create_app
from hermes.storage import SQLiteStore
from hermes.worker import UnifiedWorkerRuntime, WorkerAPI, WorkerRuntime


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

    async def test_local_agent_normalizes_crlf_on_success(self) -> None:
        settings = WorkerSettings(
            worker_id="crlf-success-worker",
            worker_name="CRLF Success Worker",
            shared_secret="worker-test-secret",
            default_agent="codex",
            command=[sys.executable, "-c", "print('unused')", "{prompt}"],
            agent_commands={
                "codex": [
                    sys.executable,
                    "-c",
                    r"import sys; sys.stdout.buffer.write(b'line1\r\nline2\r\n')",
                    "{prompt}",
                ]
            },
            allowed_workdir=self.temporary_directory.name,
        )
        await self.api.register("CRLF Success Worker", 1)
        created = self.store.create_task(
            {"prompt": "success", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        task = await self.api.claim()
        self.assertEqual(created["id"], task["id"])

        await WorkerRuntime(settings, self.api).run_task(task)

        result = self.store.get_task(created["id"])
        self.assertEqual("succeeded", result["status"])
        self.assertEqual("line1\nline2\n", result["result"])

    async def test_local_agent_normalizes_crlf_on_failure_output(self) -> None:
        settings = WorkerSettings(
            worker_id="crlf-failure-worker",
            worker_name="CRLF Failure Worker",
            shared_secret="worker-test-secret",
            default_agent="codex",
            command=[sys.executable, "-c", "print('unused')", "{prompt}"],
            agent_commands={
                "codex": [
                    sys.executable,
                    "-c",
                    r"import sys; sys.stdout.buffer.write(b'partial\r\n'); sys.stderr.buffer.write(b'failed\r\nreason\r\n'); raise SystemExit(3)",
                    "{prompt}",
                ]
            },
            allowed_workdir=self.temporary_directory.name,
        )
        await self.api.register("CRLF Failure Worker", 1)
        created = self.store.create_task(
            {"prompt": "failure", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        task = await self.api.claim()
        self.assertEqual(created["id"], task["id"])

        await WorkerRuntime(settings, self.api).run_task(task)

        result = self.store.get_task(created["id"])
        self.assertEqual("failed", result["status"])
        self.assertEqual("partial\n", result["result"])
        self.assertEqual("failed\nreason\n", result["error"])

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
        self.assertEqual(["agent:claude-code", "agent:codex"], runtime.capabilities())
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

    async def test_unified_worker_executes_local_agent_and_registers_unified_kind(
        self,
    ) -> None:
        settings = UnifiedWorkerSettings(
            coordinator_url="http://test",
            worker_id="unified-worker",
            worker_name="Unified Worker",
            shared_secret="worker-test-secret",
            default_agent="codex",
            command=[sys.executable, "-c", "print('unused')", "{prompt}"],
            agent_commands={
                "codex": [
                    sys.executable,
                    "-c",
                    "import sys; print('unified: ' + sys.argv[1])",
                    "{prompt}",
                ]
            },
            allowed_workdir=self.temporary_directory.name,
            gateway_url="",
            gateway_id="",
            poll_interval_seconds=0,
        )
        created = self.store.create_task(
            {"prompt": "unified hello", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        api = WorkerAPI(
            "http://test", "unified-worker", "worker-test-secret", self.client
        )
        await UnifiedWorkerRuntime(settings, api).run(once=True)

        result = self.store.get_task(created["id"])
        self.assertEqual("succeeded", result["status"])
        self.assertEqual("unified: unified hello\n", result["result"])
        worker = self.store.list_workers(stale_seconds=45)[-1]
        self.assertEqual("unified", worker["worker_kind"])

    async def test_unified_worker_preserves_legacy_command_only_environment(
        self,
    ) -> None:
        command = (
            f"{sys.executable} -c \"import sys; print('legacy: ' + sys.argv[1])\" "
            "{prompt}"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch.dict(
                os.environ,
                {
                    "HERMES_ENV_FILE": str(Path(temporary_directory) / "missing.env"),
                    "HERMES_COORDINATOR_URL": "http://test",
                    "HERMES_WORKER_ID": "legacy-worker",
                    "HERMES_WORKER_SHARED_SECRET": "worker-test-secret",
                    "HERMES_WORKER_COMMAND": command,
                    "HERMES_WORKER_ALLOWED_WORKDIR": self.temporary_directory.name,
                    "HERMES_WORKER_POLL_INTERVAL_SECONDS": "0",
                },
                clear=True,
            ):
                settings = UnifiedWorkerSettings.from_env()

        self.assertFalse(settings.gateway_enabled)
        created = self.store.create_task(
            {"prompt": "legacy hello", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        api = WorkerAPI(
            "http://test", "legacy-worker", "worker-test-secret", self.client
        )
        await UnifiedWorkerRuntime(settings, api).run(once=True)

        result = self.store.get_task(created["id"])
        self.assertEqual("succeeded", result["status"])
        self.assertEqual("legacy: legacy hello\n", result["result"])

    async def test_local_agent_process_does_not_receive_hermes_control_secrets(
        self,
    ) -> None:
        script = (
            "import json, os; print(json.dumps({"
            "'worker_secret': bool(os.getenv('HERMES_WORKER_SHARED_SECRET')),"
            "'director_key': bool(os.getenv('HERMES_DIRECTOR_API_KEY')),"
            "'gateway_token': bool(os.getenv('HERMES_GATEWAY_TOKEN')),"
            "'profile_keys': bool(os.getenv('HERMES_GATEWAY_PROFILE_KEYS_JSON')),"
            "'safe_setting': os.getenv('HERMES_SAFE_SETTING'),"
            "'provider_key': os.getenv('OPENAI_API_KEY')"
            "}, sort_keys=True))"
        )
        settings = WorkerSettings(
            coordinator_url="http://test",
            worker_id="mock-worker",
            worker_name="Mock Worker",
            shared_secret="worker-test-secret",
            default_agent="codex",
            command=[sys.executable, "-c", "print('unused')", "{prompt}"],
            agent_commands={
                "codex": [sys.executable, "-c", script, "{prompt}"],
            },
            allowed_workdir=self.temporary_directory.name,
        )
        await self.api.register("Mock Worker", 1)
        created = self.store.create_task(
            {"prompt": "inspect environment", "timeout_seconds": 10},
            default_timeout_seconds=10,
            max_timeout_seconds=10,
            default_max_attempts=1,
        )
        task = await self.api.claim()
        self.assertEqual(created["id"], task["id"])
        with patch.dict(
            os.environ,
            {
                "HERMES_WORKER_SHARED_SECRET": "worker-secret",
                "HERMES_DIRECTOR_API_KEY": "director-key",
                "HERMES_GATEWAY_TOKEN": "gateway-token",
                "HERMES_GATEWAY_PROFILE_KEYS_JSON": '{"default":"profile-key"}',
                "HERMES_SAFE_SETTING": "visible",
                "OPENAI_API_KEY": "provider-key",
            },
            clear=True,
        ):
            await WorkerRuntime(settings, self.api).run_task(task)

        result = self.store.get_task(created["id"])
        self.assertEqual("succeeded", result["status"])
        self.assertEqual(
            {
                "director_key": False,
                "gateway_token": False,
                "profile_keys": False,
                "provider_key": "provider-key",
                "safe_setting": "visible",
                "worker_secret": False,
            },
            json.loads(result["result"]),
        )


if __name__ == "__main__":
    unittest.main()
