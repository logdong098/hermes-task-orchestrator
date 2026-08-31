from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

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


def main() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    source_root = repository_root / "src"
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory() as temporary_directory:
        environment = os.environ.copy()
        environment.update(
            {
                "PYTHONPATH": str(source_root),
                "PYTHONDONTWRITEBYTECODE": "1",
                "HERMES_DATABASE_PATH": str(Path(temporary_directory) / "hermes.db"),
                "HERMES_DIRECTOR_API_KEY": "e2e-director-key",
                "HERMES_WORKER_SHARED_SECRET": "e2e-worker-secret",
                "HERMES_COORDINATOR_URL": base_url,
                "HERMES_WORKER_ID": "e2e-worker",
                "HERMES_WORKER_NAME": "E2E Mock Worker",
                "HERMES_WORKER_ALLOWED_WORKDIR": temporary_directory,
                "HERMES_WORKER_COMMAND": (
                    f"{sys.executable} -m hermes.mock_hermes -q {{prompt}}"
                ),
                "HERMES_WORKER_POLL_INTERVAL_SECONDS": "1",
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
                str(port),
                "--log-level",
                "warning",
            ],
            env=environment,
        )
        try:
            wait_until_healthy(base_url)
            headers = {"Authorization": "Bearer e2e-director-key"}
            created = httpx.post(
                f"{base_url}/api/v1/tasks",
                json={"prompt": "end-to-end hello", "timeout_seconds": 10},
                headers=headers,
                timeout=5,
            )
            created.raise_for_status()
            task_id = created.json()["id"]
            worker = subprocess.run(
                [sys.executable, "-m", "hermes.worker", "--once"],
                env=environment,
                timeout=20,
                check=False,
                capture_output=True,
                text=True,
            )
            if worker.returncode != 0:
                raise RuntimeError(f"worker failed: {worker.stderr}")
            result = httpx.get(
                f"{base_url}/api/v1/tasks/{task_id}", headers=headers, timeout=5
            )
            result.raise_for_status()
            task = result.json()
            if task["status"] != "succeeded":
                raise RuntimeError(f"task did not succeed: {task}")
            print(
                f"E2E OK task={task_id} status={task['status']} "
                f"result={task['result'].strip()}"
            )
        finally:
            coordinator.terminate()
            try:
                coordinator.wait(timeout=5)
            except subprocess.TimeoutExpired:
                coordinator.kill()
                coordinator.wait(timeout=5)


if __name__ == "__main__":
    main()
