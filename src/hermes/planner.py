from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .models import TERMINAL_STATUSES, TaskStatus
from .storage import ConflictError, SQLiteStore

LOGGER = logging.getLogger("hermes.planner")
TRUNCATION_MARKER = b"\n[output truncated]"
SENSITIVE_HERMES_ENV_MARKERS = ("KEY", "SECRET", "TOKEN", "PASSWORD")


@dataclass(frozen=True)
class PlannerSettings:
    agent_commands: Dict[str, List[str]]
    default_agent: str = "codex"
    timeout_seconds: float = 900
    max_output_bytes: int = 2_000_000
    poll_interval_seconds: float = 2.0
    lease_seconds: int = 30


class PlannerRuntime:
    """Coordinator-local runtime that turns task prompts into execution plans."""

    def __init__(self, settings: PlannerSettings, store: SQLiteStore) -> None:
        self.settings = settings
        self.store = store
        self.processes: Dict[str, asyncio.subprocess.Process] = {}
        self._validate_settings()

    def _validate_settings(self) -> None:
        if not self.settings.default_agent:
            raise ValueError("default planner agent must not be empty")
        if self.settings.default_agent not in ("claude", "codex"):
            raise ValueError("default planner agent must be claude or codex")
        if self.settings.default_agent not in self.settings.agent_commands:
            raise ValueError("default planner agent has no command")
        if self.settings.timeout_seconds <= 0:
            raise ValueError("planner timeout must be positive")
        if self.settings.max_output_bytes < 1:
            raise ValueError("planner output limit must be positive")
        for agent, command in self.settings.agent_commands.items():
            if not agent or not command:
                raise ValueError("planner commands must have non-empty names and argv")
            if "{prompt}" not in command:
                raise ValueError(
                    f"planner command for {agent} must contain a {{prompt}} argument"
                )
            if any("dangerously-bypass" in argument.lower() for argument in command):
                raise ValueError("dangerously-bypass options are forbidden")

    def command_for(self, prompt: str, agent: Optional[str]) -> List[str]:
        selected = agent or self.settings.default_agent
        command = self.settings.agent_commands.get(selected)
        if command is None:
            raise ValueError(f"unknown planner agent: {selected}")
        return [prompt if argument == "{prompt}" else argument for argument in command]

    @staticmethod
    def subprocess_environment() -> Dict[str, str]:
        """Keep provider auth while excluding Coordinator-owned Hermes secrets."""
        return {
            key: value
            for key, value in os.environ.items()
            if not (
                key.startswith("HERMES_")
                and any(
                    marker in key.upper() for marker in SENSITIVE_HERMES_ENV_MARKERS
                )
            )
        }

    @staticmethod
    async def _read_limited(
        stream: Optional[asyncio.StreamReader], limit: int
    ) -> bytes:
        if stream is None:
            return b""
        captured = bytearray()
        truncated = False
        while True:
            chunk = await stream.read(64 * 1024)
            if not chunk:
                break
            remaining = limit - len(captured)
            if remaining > 0:
                captured.extend(chunk[:remaining])
            if len(chunk) > max(remaining, 0):
                truncated = True
        if truncated:
            captured.extend(TRUNCATION_MARKER)
        return bytes(captured)

    async def run(self, stopping: asyncio.Event) -> None:
        while not stopping.is_set():
            task = self.store.claim_planning_task(self.settings.lease_seconds)
            if task is not None:
                await self.run_task(task)
                continue
            try:
                await asyncio.wait_for(
                    stopping.wait(),
                    timeout=max(self.settings.poll_interval_seconds, 0.1),
                )
            except asyncio.TimeoutError:
                pass

    async def run_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_id = task["id"]
        requested_agent = task.get("planner_agent")
        if task["status"] == TaskStatus.PLANNING_PENDING.value:
            claimed = self.store.claim_planning_task(
                self.settings.lease_seconds, task_id=task_id
            )
            if claimed is None:
                return self.store.get_task(task_id)
            task = claimed
        elif task["status"] != TaskStatus.PLANNING.value:
            raise ConflictError(f"task {task_id} is not ready for planning")
        planner_attempt_count = int(task["planner_attempt_count"])

        lease_task: Optional[asyncio.Task[None]] = None
        try:
            command = self.command_for(task["prompt"], requested_agent)
            process_options: Dict[str, Any] = {}
            if os.name == "posix":
                process_options["start_new_session"] = True
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self.subprocess_environment(),
                **process_options,
            )
            self.processes[task_id] = process
            current = self.store.get_task(task_id)
            if current["status"] in TERMINAL_STATUSES:
                await self.cancel_task(task_id)
                return current
            lease_task = asyncio.create_task(
                self._renew_lease(task_id, planner_attempt_count)
            )
            stdout_task = asyncio.create_task(
                self._read_limited(process.stdout, self.settings.max_output_bytes)
            )
            stderr_task = asyncio.create_task(
                self._read_limited(process.stderr, self.settings.max_output_bytes)
            )
            timed_out = False
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=self.settings.timeout_seconds
                )
            except asyncio.TimeoutError:
                timed_out = True
                await self.cancel_task(task_id)
                await process.wait()
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)

            current = self.store.get_task(task_id)
            if current["status"] in TERMINAL_STATUSES:
                return current
            if timed_out:
                return self.store.fail_planning(
                    task_id,
                    TaskStatus.TIMED_OUT.value,
                    f"planner exceeded {self.settings.timeout_seconds:g} seconds",
                    retryable=True,
                    expected_attempt_count=planner_attempt_count,
                )
            output = stdout.decode("utf-8", errors="replace").strip()
            error_output = stderr.decode("utf-8", errors="replace").strip()
            if process.returncode != 0:
                return self.store.fail_planning(
                    task_id,
                    TaskStatus.FAILED.value,
                    error_output or f"planner exited with {process.returncode}",
                    retryable=True,
                    expected_attempt_count=planner_attempt_count,
                )
            if not output:
                return self.store.fail_planning(
                    task_id,
                    TaskStatus.FAILED.value,
                    "planner returned an empty plan",
                    retryable=True,
                    expected_attempt_count=planner_attempt_count,
                )
            execution_prompt = self._execution_prompt(task["prompt"], output)
            return self.store.complete_planning(
                task_id,
                output,
                execution_prompt,
                expected_attempt_count=planner_attempt_count,
            )
        except (FileNotFoundError, PermissionError, ValueError) as exc:
            return self.store.fail_planning(
                task_id,
                TaskStatus.FAILED.value,
                f"planner execution error: {exc}",
                expected_attempt_count=planner_attempt_count,
            )
        except ConflictError:
            return self.store.get_task(task_id)
        except asyncio.CancelledError:
            await self.cancel_task(task_id)
            raise
        except Exception as exc:
            LOGGER.exception("planner failed unexpectedly for task %s", task_id)
            return self.store.fail_planning(
                task_id,
                TaskStatus.FAILED.value,
                f"planner internal error: {exc}",
                retryable=True,
                expected_attempt_count=planner_attempt_count,
            )
        finally:
            if lease_task is not None:
                lease_task.cancel()
                await asyncio.gather(lease_task, return_exceptions=True)
            self.processes.pop(task_id, None)

    async def _renew_lease(self, task_id: str, planner_attempt_count: int) -> None:
        interval = max(self.settings.lease_seconds / 3, 0.1)
        while True:
            await asyncio.sleep(interval)
            if not self.store.extend_planning_lease(
                task_id,
                self.settings.lease_seconds,
                expected_attempt_count=planner_attempt_count,
            ):
                return

    async def cancel_task(self, task_id: str) -> None:
        process = self.processes.get(task_id)
        if process is None or process.returncode is not None:
            return
        await self._signal_process(process, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            await self._signal_process(process, signal.SIGKILL)
            await process.wait()

    @staticmethod
    async def _signal_process(
        process: asyncio.subprocess.Process, requested_signal: signal.Signals
    ) -> None:
        if isinstance(process, asyncio.subprocess.Process) and os.name == "posix":
            try:
                os.killpg(process.pid, requested_signal)
                return
            except ProcessLookupError:
                return
        action = (
            process.terminate if requested_signal == signal.SIGTERM else process.kill
        )
        result = action()
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def _execution_prompt(prompt: str, plan: str) -> str:
        return (
            "Original development task:\n"
            f"{prompt}\n\n"
            "Coordinator execution plan:\n"
            f"{plan}\n\n"
            "Implement the task in the assigned workspace and verify the result."
        )
