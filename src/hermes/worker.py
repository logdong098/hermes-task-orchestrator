from __future__ import annotations

import argparse
import asyncio
import logging
import os
import platform
import signal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx

from .config import WorkerSettings
from .security import compact_json, sign_request

LOGGER = logging.getLogger("hermes.worker")
MAX_CAPTURE_BYTES = 2_000_000
TRUNCATION_MARKER = b"\n[output truncated by Hermes Worker]\n"


class WorkerAPI:
    def __init__(
        self,
        base_url: str,
        worker_id: str,
        shared_secret: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.shared_secret = shared_secret
        self.client = client or httpx.AsyncClient(timeout=30)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        body = compact_json(payload)
        headers = sign_request(self.shared_secret, method, path, body)
        headers["X-Hermes-Worker-ID"] = self.worker_id
        if payload is not None:
            headers["Content-Type"] = "application/json"
        response = await self.client.request(
            method,
            f"{self.base_url}{path}",
            content=body,
            headers=headers,
        )
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()

    async def register(
        self,
        name: str,
        max_concurrency: int,
        capabilities: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        worker_kind: str = "command",
        default_agent: str = "default",
        routes: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        registration_metadata = {
            "platform": platform.platform(),
            "python": platform.python_version(),
        }
        if metadata:
            registration_metadata.update(metadata)
        return await self._request(
            "POST",
            "/api/v1/workers/register",
            {
                "worker_id": self.worker_id,
                "name": name,
                "max_concurrency": max_concurrency,
                "capabilities": capabilities or ["hermes-chat"],
                "metadata": registration_metadata,
                "worker_kind": worker_kind,
                "default_agent": default_agent,
                "routes": routes or [],
            },
        )

    async def heartbeat(self, running_claims: List[Dict[str, str]]) -> List[str]:
        response = await self._request(
            "POST",
            f"/api/v1/workers/{self.worker_id}/heartbeat",
            {"running_claims": running_claims},
        )
        return response.get("cancel_task_ids", [])

    async def claim(self) -> Optional[Dict[str, Any]]:
        response = await self._request(
            "POST",
            f"/api/v1/workers/{self.worker_id}/tasks/claim",
        )
        return response.get("task")

    async def set_running(self, task_id: str, claim_token: str) -> Dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/tasks/{task_id}/status",
            {"status": "running", "claim_token": claim_token},
        )

    async def progress(
        self,
        task_id: str,
        claim_token: str,
        phase: str,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/tasks/{task_id}/progress",
            {
                "claim_token": claim_token,
                "phase": phase,
                "message": message,
                "details": details or {},
            },
        )

    async def reconcile(
        self,
        task_id: str,
        claim_token: str,
        reason: str,
        deadline_exceeded: bool = False,
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/tasks/{task_id}/reconcile",
            {
                "claim_token": claim_token,
                "reason": reason,
                "deadline_exceeded": deadline_exceeded,
            },
        )

    async def report(
        self,
        task_id: str,
        claim_token: str,
        status: str,
        result: Optional[str] = None,
        error: Optional[str] = None,
        retryable: bool = False,
        remote_run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/tasks/{task_id}/result",
            {
                "claim_token": claim_token,
                "status": status,
                "result": result,
                "error": error,
                "retryable": retryable,
                "remote_run_id": remote_run_id,
            },
        )


class WorkerRuntime:
    def __init__(self, settings: WorkerSettings, api: WorkerAPI) -> None:
        self.settings = settings
        self.api = api
        self.allowed_workdir = Path(settings.allowed_workdir).resolve()
        self.active: Dict[str, asyncio.Task[None]] = {}
        self.claim_tokens: Dict[str, str] = {}
        self.processes: Dict[str, asyncio.subprocess.Process] = {}
        self.cancelled: Set[str] = set()
        self.stopping = asyncio.Event()
        self._validate_settings()

    def _validate_settings(self) -> None:
        if not self.settings.worker_id:
            raise ValueError("HERMES_WORKER_ID is required")
        if not self.settings.shared_secret:
            raise ValueError("HERMES_WORKER_SHARED_SECRET is required")
        if not self.settings.default_agent:
            raise ValueError("default agent must not be empty")
        commands = dict(self.settings.agent_commands)
        if not commands:
            commands[self.settings.default_agent] = self.settings.command
        elif self.settings.default_agent not in commands:
            # HERMES_WORKER_COMMAND remains the fallback for the default agent.
            commands[self.settings.default_agent] = self.settings.command
        for agent, command in commands.items():
            if not agent or not command:
                raise ValueError("agent commands must have non-empty names and argv")
            lowered = [part.lower() for part in command]
            if any("dangerously-bypass" in part for part in lowered):
                raise ValueError("dangerously-bypass options are forbidden")
            if "{prompt}" not in command:
                raise ValueError(
                    f"command for agent {agent} must contain a {{prompt}} argument"
                )
        if not self.allowed_workdir.is_dir():
            raise ValueError("allowed work directory does not exist")
        if self.settings.concurrency < 1:
            raise ValueError("worker concurrency must be at least 1")

    def resolve_workdir(self, requested: Optional[str]) -> Path:
        candidate = (
            self.allowed_workdir
            if not requested
            else (self.allowed_workdir / requested).resolve()
        )
        try:
            candidate.relative_to(self.allowed_workdir)
        except ValueError as exc:
            raise ValueError("requested workdir escapes allowed root") from exc
        if not candidate.is_dir():
            raise ValueError("requested workdir does not exist")
        return candidate

    def command_for(self, prompt: str, agent: Optional[str] = None) -> List[str]:
        selected_agent = agent or self.settings.default_agent
        command = self.settings.agent_commands.get(selected_agent)
        if command is None:
            if selected_agent == self.settings.default_agent:
                command = self.settings.command
            else:
                raise ValueError(f"unknown execution agent: {selected_agent}")
        return [prompt if argument == "{prompt}" else argument for argument in command]

    def capabilities(self) -> List[str]:
        agents = set(self.settings.agent_commands)
        agents.add(self.settings.default_agent)
        return [f"agent:{agent}" for agent in sorted(agents)]

    async def _progress(
        self,
        task_id: str,
        claim_token: str,
        phase: str,
        message: Optional[str] = None,
    ) -> None:
        reporter = getattr(self.api, "progress", None)
        if reporter is None:
            return
        try:
            await reporter(task_id, claim_token, phase, message)
        except httpx.HTTPError:
            LOGGER.warning("could not report progress for task %s", task_id)

    @staticmethod
    async def _read_limited(
        stream: Optional[asyncio.StreamReader], limit: int = MAX_CAPTURE_BYTES
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
            if len(chunk) > remaining:
                truncated = True
        if truncated:
            captured.extend(TRUNCATION_MARKER)
        return bytes(captured)

    async def run_task(self, task: Dict[str, Any]) -> None:
        task_id = task["id"]
        claim_token = task["claim_token"]
        try:
            if task_id in self.cancelled:
                await self.api.report(
                    task_id,
                    claim_token,
                    "cancelled",
                    error="cancelled before start",
                )
                return
            try:
                await self.api.set_running(task_id, claim_token)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 409:
                    await self.api.report(
                        task_id,
                        claim_token,
                        "cancelled",
                        error="cancelled before start",
                    )
                    return
                raise
            await self._progress(
                task_id, claim_token, "preparing", "preparing local execution"
            )
            workdir = self.resolve_workdir(task.get("workdir"))
            prompt = task.get("execution_prompt") or task["prompt"]
            command = self.command_for(prompt, task.get("execution_agent"))
            LOGGER.info("starting task %s in %s", task_id, workdir)
            process_options: Dict[str, Any] = {}
            if os.name == "posix":
                process_options["start_new_session"] = True
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=os.environ.copy(),
                **process_options,
            )
            self.processes[task_id] = process
            await self._progress(
                task_id, claim_token, "process_started", "local process started"
            )
            stdout_task = asyncio.create_task(self._read_limited(process.stdout))
            stderr_task = asyncio.create_task(self._read_limited(process.stderr))
            if task_id in self.cancelled:
                await self._terminate_process(task_id)
            timeout = min(
                int(task["timeout_seconds"]),
                self.settings.task_timeout_seconds,
            )
            timed_out = False
            try:
                await asyncio.wait_for(process.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                timed_out = True
                await self._terminate_process(task_id)
            await self._progress(
                task_id,
                claim_token,
                "collecting_result",
                "collecting process output",
            )
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
            if timed_out:
                await self.api.report(
                    task_id,
                    claim_token,
                    "timed_out",
                    error=f"local execution exceeded {timeout} seconds",
                )
                return
            output = stdout.decode("utf-8", errors="replace")
            error_output = stderr.decode("utf-8", errors="replace")
            if task_id in self.cancelled:
                await self.api.report(
                    task_id,
                    claim_token,
                    "cancelled",
                    error="cancelled by director",
                )
            elif process.returncode == 0:
                await self.api.report(task_id, claim_token, "succeeded", result=output)
            else:
                await self.api.report(
                    task_id,
                    claim_token,
                    "failed",
                    result=output or None,
                    error=error_output or f"command exited with {process.returncode}",
                    retryable=True,
                )
        except (FileNotFoundError, PermissionError, ValueError) as exc:
            await self.api.report(
                task_id,
                claim_token,
                "failed",
                error=f"worker execution error: {exc}",
                retryable=False,
            )
        except httpx.HTTPError:
            LOGGER.exception("coordinator communication failed for task %s", task_id)
        except Exception:
            LOGGER.exception("task %s failed unexpectedly", task_id)
            try:
                await self.api.report(
                    task_id,
                    claim_token,
                    "failed",
                    error="worker internal error; inspect worker logs",
                    retryable=False,
                )
            except Exception:
                LOGGER.exception("could not report failure for task %s", task_id)
        finally:
            self.processes.pop(task_id, None)
            self.cancelled.discard(task_id)
            LOGGER.info("task %s finished", task_id)

    async def _terminate_process(self, task_id: str) -> None:
        process = self.processes.get(task_id)
        if process is None or process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            await process.wait()
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

    async def cancel_task(self, task_id: str) -> None:
        self.cancelled.add(task_id)
        await self._terminate_process(task_id)

    async def heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                running_claims = [
                    {"task_id": task_id, "claim_token": claim_token}
                    for task_id, claim_token in self.claim_tokens.items()
                    if task_id in self.active
                ]
                cancel_task_ids = await self.api.heartbeat(running_claims)
                for task_id in cancel_task_ids:
                    await self.cancel_task(task_id)
            except Exception:
                LOGGER.exception("heartbeat failed")
            try:
                await asyncio.wait_for(
                    self.stopping.wait(),
                    timeout=self.settings.heartbeat_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass

    def _task_done(self, task_id: str, _: asyncio.Task[None]) -> None:
        self.active.pop(task_id, None)
        self.claim_tokens.pop(task_id, None)

    async def run(self, once: bool = False) -> None:
        await self.api.register(
            self.settings.worker_name,
            self.settings.concurrency,
            capabilities=self.capabilities(),
            metadata={"default_agent": self.settings.default_agent},
            worker_kind="command",
            default_agent=self.settings.default_agent,
        )
        LOGGER.info("worker %s registered", self.settings.worker_id)
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        try:
            while not self.stopping.is_set():
                claimed_any = False
                while len(self.active) < self.settings.concurrency:
                    task = await self.api.claim()
                    if task is None:
                        break
                    claimed_any = True
                    task_id = task["id"]
                    self.claim_tokens[task_id] = task["claim_token"]
                    running = asyncio.create_task(self.run_task(task))
                    self.active[task_id] = running
                    running.add_done_callback(
                        lambda done, identifier=task_id: self._task_done(
                            identifier, done
                        )
                    )
                if once and not self.active and not claimed_any:
                    break
                await asyncio.sleep(self.settings.poll_interval_seconds)
        finally:
            self.stopping.set()
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            if self.active:
                await asyncio.gather(*self.active.values(), return_exceptions=True)


async def run_worker(settings: WorkerSettings, once: bool = False) -> None:
    api = WorkerAPI(
        settings.coordinator_url,
        settings.worker_id,
        settings.shared_secret,
    )
    runtime = WorkerRuntime(settings, api)
    try:
        await runtime.run(once=once)
    finally:
        await api.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Hermes Worker")
    parser.add_argument(
        "--once", action="store_true", help="exit after the queue is drained"
    )
    parser.add_argument("--log-level", default="INFO")
    arguments = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, arguments.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker(WorkerSettings.from_env(), once=arguments.once))


if __name__ == "__main__":
    main()
