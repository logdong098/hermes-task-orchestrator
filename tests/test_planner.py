from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hermes.config import CoordinatorSettings
from hermes.planner import PlannerRuntime, PlannerSettings
from hermes.storage import SQLiteStore


class FakeProcess:
    def __init__(self, stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0):
        self.stdout_bytes = stdout
        self.stderr_bytes = stderr
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stdout.feed_data(stdout)
        self.stdout.feed_eof()
        self.stderr.feed_data(stderr)
        self.stderr.feed_eof()
        self.returncode = returncode
        self.pid = 1234
        self.terminate = AsyncMock()
        self.kill = AsyncMock()

    async def communicate(self) -> tuple[bytes, bytes]:
        return self.stdout_bytes, self.stderr_bytes

    async def wait(self) -> int:
        return self.returncode


class BlockingProcess(FakeProcess):
    def __init__(self):
        super().__init__()
        self.exited = asyncio.Event()
        self.returncode = None

        async def terminate() -> None:
            self.exited.set()

        self.terminate = AsyncMock(side_effect=terminate)

    async def wait(self) -> int:
        await self.exited.wait()
        self.returncode = -15
        return self.returncode


class PlannerRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        self.store = SQLiteStore(database)
        self.store.initialize()
        self.task = self.store.create_task(
            {
                "prompt": "add a health endpoint",
                "planner_agent": "claude",
                "execution_agent": "worker-a",
            },
            default_timeout_seconds=60,
            max_timeout_seconds=600,
            default_max_attempts=1,
            default_planner_max_attempts=1,
        )
        self.settings = PlannerSettings(
            agent_commands={
                "claude": ["claude", "-p", "{prompt}"],
                "codex": ["codex", "exec", "{prompt}"],
            },
            default_agent="claude",
            timeout_seconds=5,
            max_output_bytes=1024,
        )

    async def asyncTearDown(self) -> None:
        self.temporary_directory.cleanup()

    async def test_selected_agent_template_receives_original_prompt(self) -> None:
        runtime = PlannerRuntime(self.settings, self.store)
        process = FakeProcess(b'{"plan":"use tests"}')
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ) as execute:
            await runtime.run_task({**self.task, "planner_agent": "codex"})

        self.assertEqual(
            ("codex", "exec", "add a health endpoint"), execute.call_args.args
        )

    async def test_default_planner_is_limited_to_claude_or_codex(self) -> None:
        with self.assertRaises(ValueError):
            PlannerRuntime(
                PlannerSettings(
                    agent_commands={"custom": ["custom", "{prompt}"]},
                    default_agent="custom",
                ),
                self.store,
            )

    async def test_default_planner_commands_are_read_only(self) -> None:
        commands = CoordinatorSettings().planner_commands

        self.assertEqual(
            ["claude", "--permission-mode", "plan", "-p", "{prompt}"],
            commands["claude"],
        )
        self.assertEqual(
            ["codex", "exec", "--sandbox", "read-only", "{prompt}"],
            commands["codex"],
        )

    async def test_planner_environment_excludes_coordinator_secrets(self) -> None:
        runtime = PlannerRuntime(self.settings, self.store)
        with patch.dict(
            "os.environ",
            {
                "HERMES_DIRECTOR_API_KEY": "director-secret",
                "HERMES_WORKER_SHARED_SECRET": "worker-secret",
                "HERMES_GATEWAY_PROFILE_KEYS_JSON": '{"dev":"gateway-secret"}',
                "OPENAI_API_KEY": "planner-provider-key",
            },
        ):
            environment = runtime.subprocess_environment()
        self.assertNotIn("HERMES_DIRECTOR_API_KEY", environment)
        self.assertNotIn("HERMES_WORKER_SHARED_SECRET", environment)
        self.assertNotIn("HERMES_GATEWAY_PROFILE_KEYS_JSON", environment)
        self.assertEqual("planner-provider-key", environment["OPENAI_API_KEY"])

    async def test_successful_plan_is_persisted_and_task_returns_to_pending(
        self,
    ) -> None:
        runtime = PlannerRuntime(self.settings, self.store)
        process = FakeProcess(b'{"steps":["write test","implement"]}')
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            result = await runtime.run_task(self.task)

        self.assertEqual("pending", result["status"])
        self.assertEqual('{"steps":["write test","implement"]}', result["plan"])
        stored = self.store.get_task(self.task["id"])
        self.assertEqual("pending", stored["status"])
        self.assertEqual(result["plan"], stored["plan"])

    async def test_nonzero_planner_exit_marks_task_failed(self) -> None:
        runtime = PlannerRuntime(self.settings, self.store)
        process = FakeProcess(stderr=b"invalid request", returncode=2)
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            result = await runtime.run_task(self.task)

        self.assertEqual("failed", result["status"])
        self.assertIn("invalid request", result["error"])

    async def test_planner_timeout_terminates_process_and_marks_task_timed_out(
        self,
    ) -> None:
        settings = PlannerSettings(
            agent_commands=self.settings.agent_commands,
            default_agent="claude",
            timeout_seconds=0.01,
            max_output_bytes=1024,
        )
        runtime = PlannerRuntime(settings, self.store)
        process = BlockingProcess()
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            result = await runtime.run_task(self.task)

        self.assertEqual("timed_out", result["status"])
        process.terminate.assert_awaited_once()

    async def test_planner_output_is_bounded_and_marks_truncation(self) -> None:
        settings = PlannerSettings(
            agent_commands=self.settings.agent_commands,
            default_agent="claude",
            timeout_seconds=5,
            max_output_bytes=8,
        )
        runtime = PlannerRuntime(settings, self.store)
        process = FakeProcess(b"x" * 64)
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            result = await runtime.run_task(self.task)

        self.assertLessEqual(len(result["plan"]), 8 + len("\n[output truncated]"))
        self.assertIn("output truncated", result["plan"])

    async def test_cancel_task_terminates_active_planner_process(self) -> None:
        runtime = PlannerRuntime(self.settings, self.store)
        process = BlockingProcess()
        runtime.processes[self.task["id"]] = process

        await runtime.cancel_task(self.task["id"])

        process.terminate.assert_awaited_once()

    async def test_retryable_failure_requeues_then_exhausts_budget(self) -> None:
        task = self.store.create_task(
            {"prompt": "retry plan", "planner_agent": "codex"},
            default_timeout_seconds=60,
            max_timeout_seconds=600,
            default_max_attempts=1,
            default_planner_max_attempts=2,
        )
        runtime = PlannerRuntime(self.settings, self.store)
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(
                side_effect=[
                    FakeProcess(stderr=b"temporary", returncode=1),
                    FakeProcess(stderr=b"still broken", returncode=1),
                ]
            ),
        ):
            first = await runtime.run_task(task)
            second = await runtime.run_task(first)

        self.assertEqual("planning_pending", first["status"])
        self.assertEqual("failed", second["status"])
        self.assertEqual(2, second["planner_attempt_count"])

    async def test_timeout_requeues_then_exhausts_budget(self) -> None:
        task = self.store.create_task(
            {"prompt": "retry timeout", "planner_agent": "codex"},
            default_timeout_seconds=60,
            max_timeout_seconds=600,
            default_max_attempts=1,
            default_planner_max_attempts=2,
        )
        settings = PlannerSettings(
            agent_commands=self.settings.agent_commands,
            default_agent="claude",
            timeout_seconds=0.01,
            max_output_bytes=1024,
        )
        runtime = PlannerRuntime(settings, self.store)
        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=[BlockingProcess(), BlockingProcess()]),
        ):
            first = await runtime.run_task(task)
            second = await runtime.run_task(first)

        self.assertEqual("planning_pending", first["status"])
        self.assertEqual("timed_out", second["status"])

    async def test_cancellation_during_spawn_terminates_new_process(self) -> None:
        runtime = PlannerRuntime(self.settings, self.store)
        process = BlockingProcess()

        async def cancel_during_spawn(*args, **kwargs):
            self.store.cancel_task(self.task["id"])
            return process

        with patch(
            "hermes.planner.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=cancel_during_spawn),
        ):
            result = await runtime.run_task(self.task)

        self.assertEqual("cancelled", result["status"])
        process.terminate.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
