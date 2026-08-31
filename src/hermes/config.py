from __future__ import annotations

import json
import os
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


def load_env_file(path: Optional[str] = None) -> None:
    env_path = Path(path or os.getenv("HERMES_ENV_FILE", ".env"))
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _csv_int(name: str) -> List[int]:
    value = os.getenv(name, "")
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _json_dict(name: str) -> Dict[str, str]:
    value = os.getenv(name, "").strip()
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in parsed.items()
    ):
        raise ValueError(f"{name} must be a JSON object of string values")
    return parsed


@dataclass(frozen=True)
class CoordinatorSettings:
    database_path: str = "data/hermes.db"
    director_api_key: str = ""
    worker_shared_secret: str = ""
    worker_secrets: Dict[str, str] = field(default_factory=dict)
    worker_stale_seconds: int = 45
    task_lease_seconds: int = 30
    hmac_max_clock_skew_seconds: int = 300
    default_task_timeout_seconds: int = 900
    max_task_timeout_seconds: int = 7200
    default_max_attempts: int = 2
    maintenance_interval_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> "CoordinatorSettings":
        load_env_file()
        return cls(
            database_path=os.getenv("HERMES_DATABASE_PATH", "data/hermes.db"),
            director_api_key=os.getenv("HERMES_DIRECTOR_API_KEY", ""),
            worker_shared_secret=os.getenv("HERMES_WORKER_SHARED_SECRET", ""),
            worker_secrets=_json_dict("HERMES_WORKER_SECRETS_JSON"),
            worker_stale_seconds=_int("HERMES_WORKER_STALE_SECONDS", 45),
            task_lease_seconds=_int("HERMES_TASK_LEASE_SECONDS", 30),
            hmac_max_clock_skew_seconds=_int("HERMES_HMAC_MAX_CLOCK_SKEW_SECONDS", 300),
            default_task_timeout_seconds=_int(
                "HERMES_DEFAULT_TASK_TIMEOUT_SECONDS", 900
            ),
            max_task_timeout_seconds=_int("HERMES_MAX_TASK_TIMEOUT_SECONDS", 7200),
            default_max_attempts=_int("HERMES_DEFAULT_MAX_ATTEMPTS", 2),
            maintenance_interval_seconds=_float(
                "HERMES_MAINTENANCE_INTERVAL_SECONDS", 5.0
            ),
        )

    def worker_secret_for(self, worker_id: str) -> str:
        if self.worker_secrets:
            return self.worker_secrets.get(worker_id, "")
        return self.worker_shared_secret


@dataclass(frozen=True)
class WorkerSettings:
    coordinator_url: str = "http://127.0.0.1:8000"
    worker_id: str = ""
    worker_name: str = ""
    shared_secret: str = ""
    command: List[str] = field(
        default_factory=lambda: ["hermes", "chat", "-q", "{prompt}"]
    )
    allowed_workdir: str = "."
    concurrency: int = 1
    task_timeout_seconds: int = 900
    heartbeat_interval_seconds: int = 10
    poll_interval_seconds: int = 2

    @classmethod
    def from_env(cls) -> "WorkerSettings":
        import socket

        load_env_file()
        hostname = socket.gethostname()
        worker_id = os.getenv("HERMES_WORKER_ID", hostname)
        command = shlex.split(
            os.getenv("HERMES_WORKER_COMMAND", "hermes chat -q {prompt}")
        )
        return cls(
            coordinator_url=os.getenv(
                "HERMES_COORDINATOR_URL", "http://127.0.0.1:8000"
            ).rstrip("/"),
            worker_id=worker_id,
            worker_name=os.getenv("HERMES_WORKER_NAME", hostname),
            shared_secret=os.getenv("HERMES_WORKER_SHARED_SECRET", ""),
            command=command,
            allowed_workdir=os.getenv("HERMES_WORKER_ALLOWED_WORKDIR", "."),
            concurrency=_int("HERMES_WORKER_CONCURRENCY", 1),
            task_timeout_seconds=_int("HERMES_WORKER_TASK_TIMEOUT_SECONDS", 900),
            heartbeat_interval_seconds=_int(
                "HERMES_WORKER_HEARTBEAT_INTERVAL_SECONDS", 10
            ),
            poll_interval_seconds=_int("HERMES_WORKER_POLL_INTERVAL_SECONDS", 2),
        )


@dataclass(frozen=True)
class TelegramSettings:
    bot_token: str = ""
    allowed_user_ids: List[int] = field(default_factory=list)
    coordinator_url: str = "http://127.0.0.1:8000"
    director_api_key: str = ""
    poll_timeout_seconds: int = 25
    notification_interval_seconds: int = 3
    offset_path: str = "data/telegram.offset"

    @classmethod
    def from_env(cls) -> "TelegramSettings":
        load_env_file()
        return cls(
            bot_token=os.getenv("HERMES_TELEGRAM_BOT_TOKEN", ""),
            allowed_user_ids=_csv_int("HERMES_TELEGRAM_ALLOWED_USER_IDS"),
            coordinator_url=os.getenv(
                "HERMES_COORDINATOR_URL", "http://127.0.0.1:8000"
            ).rstrip("/"),
            director_api_key=os.getenv("HERMES_DIRECTOR_API_KEY", ""),
            poll_timeout_seconds=_int("HERMES_TELEGRAM_POLL_TIMEOUT_SECONDS", 25),
            notification_interval_seconds=_int(
                "HERMES_TELEGRAM_NOTIFICATION_INTERVAL_SECONDS", 3
            ),
            offset_path=os.getenv(
                "HERMES_TELEGRAM_OFFSET_PATH", "data/telegram.offset"
            ),
        )


def redacted_environment() -> Dict[str, str]:
    sensitive = ("TOKEN", "SECRET", "KEY", "PASSWORD")
    return {
        key: ("***" if any(word in key.upper() for word in sensitive) else value)
        for key, value in os.environ.items()
        if key.startswith("HERMES_")
    }
