from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_until_healthy(base_url: str) -> None:
    for _ in range(50):
        try:
            if httpx.get(f"{base_url}/healthz", timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError("coordinator did not become healthy")


class FakeGatewayHandler(BaseHTTPRequestHandler):
    api_key = "e2e-profile-key"
    prompt = ""
    started = False
    cancel_started = threading.Event()
    cancel_stopped = False

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        if self.headers.get("Authorization") == f"Bearer {self.api_key}":
            return True
        self._json(401, {"error": "unauthorized"})
        return False

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/api/profiles":
            self._json(200, {"profiles": [{"name": "architect"}]})
            return
        if path == "/p/architect/v1/runs/run-e2e":
            if not self._authorized():
                return
            if not self.started:
                self._json(404, {"error": "not found"})
                return
            self._json(
                200,
                {
                    "id": "run-e2e",
                    "session_id": "session-e2e",
                    "status": "succeeded",
                    "result": f"gateway: {self.prompt}",
                },
            )
            return
        if path == "/p/architect/v1/runs/run-cancel":
            if not self._authorized():
                return
            self._json(
                200,
                {
                    "run_id": "run-cancel",
                    "session_id": "session-cancel",
                    "status": "cancelled" if self.cancel_stopped else "running",
                },
            )
            return
        self._json(404, {"error": f"unknown path: {unquote(path)}"})

    def do_POST(self) -> None:
        path = urlsplit(self.path).path
        if path == "/p/architect/v1/runs":
            if not self._authorized():
                return
            if not self.headers.get("Idempotency-Key"):
                self._json(400, {"error": "missing idempotency key"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if set(payload) != {"input"}:
                self._json(400, {"error": "expected only input"})
                return
            prompt = str(payload["input"])
            is_cancel_run = "cancel gateway hello" in prompt
            run_id = "run-cancel" if is_cancel_run else "run-e2e"
            if is_cancel_run:
                type(self).cancel_started.set()
            else:
                type(self).prompt = prompt
                type(self).started = True
            self._json(
                201,
                {
                    "run_id": run_id,
                    "status": "running",
                },
            )
            return
        if path == "/p/architect/v1/runs/run-e2e/stop":
            if not self._authorized():
                return
            self._json(200, {"id": "run-e2e", "status": "cancelled"})
            return
        if path == "/p/architect/v1/runs/run-cancel/stop":
            if not self._authorized():
                return
            type(self).cancel_stopped = True
            self._json(200, {"run_id": "run-cancel", "status": "stopping"})
            return
        self._json(404, {"error": f"unknown path: {unquote(path)}"})


def main() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    source_root = repository_root / "src"
    coordinator_port = free_port()
    gateway_port = free_port()
    coordinator_url = f"http://127.0.0.1:{coordinator_port}"
    gateway_url = f"http://127.0.0.1:{gateway_port}"
    gateway_server = ThreadingHTTPServer(
        ("127.0.0.1", gateway_port), FakeGatewayHandler
    )
    gateway_thread = threading.Thread(target=gateway_server.serve_forever, daemon=True)
    gateway_thread.start()

    with tempfile.TemporaryDirectory() as temporary_directory:
        environment = os.environ.copy()
        environment.update(
            {
                "PYTHONPATH": str(source_root),
                "PYTHONDONTWRITEBYTECODE": "1",
                "HERMES_DATABASE_PATH": str(Path(temporary_directory) / "hermes.db"),
                "HERMES_DIRECTOR_API_KEY": "e2e-director-key",
                "HERMES_WORKER_SHARED_SECRET": "e2e-worker-secret",
                "HERMES_COORDINATOR_URL": coordinator_url,
                "HERMES_GATEWAY_WORKER_ID": "e2e-gateway-worker",
                "HERMES_GATEWAY_WORKER_NAME": "E2E Gateway Worker",
                "HERMES_GATEWAY_ID": "e2e-gateway",
                "HERMES_GATEWAY_URL": gateway_url,
                "HERMES_GATEWAY_KIND": "remote",
                "HERMES_GATEWAY_PROFILES": "architect",
                "HERMES_GATEWAY_PROFILE_KEYS_JSON": json.dumps(
                    {"architect": FakeGatewayHandler.api_key}
                ),
                "HERMES_GATEWAY_WORKER_POLL_INTERVAL_SECONDS": "0.05",
                "HERMES_GATEWAY_POLL_INTERVAL_SECONDS": "0.05",
                "HERMES_GATEWAY_HEARTBEAT_INTERVAL_SECONDS": "1",
                "HERMES_PLANNER_POLL_INTERVAL_SECONDS": "0.05",
                "HERMES_PLANNER_COMMANDS_JSON": json.dumps(
                    {
                        "codex": [
                            sys.executable,
                            "-c",
                            "import sys; print('plan: ' + sys.argv[1])",
                            "{prompt}",
                        ]
                    }
                ),
            }
        )
        coordinator = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "hermes.coordinator:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(coordinator_port),
                "--log-level",
                "warning",
            ],
            env=environment,
        )
        cancel_worker = None
        try:
            wait_until_healthy(coordinator_url)
            headers = {"Authorization": "Bearer e2e-director-key"}
            created = httpx.post(
                f"{coordinator_url}/api/v1/tasks",
                json={
                    "prompt": "headless gateway hello",
                    "timeout_seconds": 10,
                    "target_gateway_id": "e2e-gateway",
                    "target_profile": "architect",
                    "execution_agent": "hermes",
                },
                headers=headers,
                timeout=5,
            )
            created.raise_for_status()
            task_id = created.json()["id"]
            for _ in range(100):
                planned = httpx.get(
                    f"{coordinator_url}/api/v1/tasks/{task_id}",
                    headers=headers,
                    timeout=5,
                )
                planned.raise_for_status()
                if planned.json()["status"] == "pending":
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError(f"task was not planned: {planned.json()}")

            worker = subprocess.run(
                [sys.executable, "-m", "hermes.gateway_worker", "--once"],
                env=environment,
                timeout=20,
                check=False,
                capture_output=True,
                text=True,
            )
            if worker.returncode != 0:
                raise RuntimeError(f"gateway worker failed: {worker.stderr}")
            result = httpx.get(
                f"{coordinator_url}/api/v1/tasks/{task_id}",
                headers=headers,
                timeout=5,
            )
            result.raise_for_status()
            task = result.json()
            expected = {
                "status": "succeeded",
                "resolved_gateway_id": "e2e-gateway",
                "resolved_profile": "architect",
                "resolved_execution_agent": "hermes",
                "remote_run_id": "run-e2e",
                "remote_session_id": "session-e2e",
            }
            mismatches = {
                key: (task.get(key), value)
                for key, value in expected.items()
                if task.get(key) != value
            }
            if mismatches:
                raise RuntimeError(f"task audit mismatch: {mismatches}; task={task}")
            if (
                not task["result"].startswith(
                    "gateway: Original development task:\nheadless gateway hello"
                )
                or "Coordinator execution plan:\nplan: headless gateway hello"
                not in task["result"]
            ):
                raise RuntimeError(f"unexpected result: {task['result']!r}")
            print(
                f"GATEWAY E2E OK task={task_id} status={task['status']} "
                f"route={task['resolved_gateway_id']}/{task['resolved_profile']} "
                f"run={task['remote_run_id']}"
            )

            cancel_created = httpx.post(
                f"{coordinator_url}/api/v1/tasks",
                json={
                    "prompt": "cancel gateway hello",
                    "timeout_seconds": 10,
                    "target_gateway_id": "e2e-gateway",
                    "target_profile": "architect",
                    "execution_agent": "hermes",
                },
                headers=headers,
                timeout=5,
            )
            cancel_created.raise_for_status()
            cancel_task_id = cancel_created.json()["id"]
            for _ in range(100):
                planned = httpx.get(
                    f"{coordinator_url}/api/v1/tasks/{cancel_task_id}",
                    headers=headers,
                    timeout=5,
                )
                planned.raise_for_status()
                if planned.json()["status"] == "pending":
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError(f"cancel task was not planned: {planned.json()}")

            cancel_worker = subprocess.Popen(
                [sys.executable, "-m", "hermes.gateway_worker", "--once"],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if not FakeGatewayHandler.cancel_started.wait(timeout=5):
                cancel_worker.terminate()
                raise RuntimeError("cancel run did not start")
            cancelled = httpx.post(
                f"{coordinator_url}/api/v1/tasks/{cancel_task_id}/cancel",
                headers=headers,
                timeout=5,
            )
            cancelled.raise_for_status()
            _, cancel_stderr = cancel_worker.communicate(timeout=20)
            if cancel_worker.returncode != 0:
                raise RuntimeError(f"cancel gateway worker failed: {cancel_stderr}")
            cancel_result = httpx.get(
                f"{coordinator_url}/api/v1/tasks/{cancel_task_id}",
                headers=headers,
                timeout=5,
            )
            cancel_result.raise_for_status()
            cancelled_task = cancel_result.json()
            if cancelled_task["status"] != "cancelled":
                raise RuntimeError(f"Gateway task was not cancelled: {cancelled_task}")
            if not FakeGatewayHandler.cancel_stopped:
                raise RuntimeError("Gateway stop endpoint was not called")
            print(
                f"GATEWAY CANCEL E2E OK task={cancel_task_id} "
                f"status={cancelled_task['status']} run={cancelled_task['remote_run_id']}"
            )
        finally:
            if cancel_worker is not None and cancel_worker.poll() is None:
                cancel_worker.terminate()
                try:
                    cancel_worker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    cancel_worker.kill()
                    cancel_worker.wait(timeout=5)
            coordinator.terminate()
            try:
                coordinator.wait(timeout=5)
            except subprocess.TimeoutExpired:
                coordinator.kill()
                coordinator.wait(timeout=5)
            gateway_server.shutdown()
            gateway_server.server_close()
            gateway_thread.join(timeout=5)


if __name__ == "__main__":
    main()
