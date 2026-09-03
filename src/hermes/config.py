from __future__ import annotations

import json
import os
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .models import canonical_agent_name


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


def _json_agent_commands(name: str) -> Dict[str, List[str]]:
    """Read agent command templates from a JSON object.

    Values may be argv arrays (recommended) or shell-style strings for
    convenience.  Commands are still executed with create_subprocess_exec;
    strings are only tokenized here and never passed through a shell.
    """
    value = os.getenv(name, "").strip()
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must be a JSON object of agent commands")
    commands: Dict[str, List[str]] = {}
    for agent, command in parsed.items():
        if not isinstance(agent, str):
            raise ValueError(f"{name} agent names must be strings")
        if isinstance(command, str):
            argv = shlex.split(command)
        elif isinstance(command, list) and all(
            isinstance(item, str) for item in command
        ):
            argv = list(command)
        else:
            raise ValueError(f"{name} values must be argv arrays or strings")
        commands[agent] = argv
    return commands


def _default_execution_commands() -> Dict[str, List[str]]:
    """Commands used by the unified worker when no override is supplied."""
    return {
        "codex": ["codex", "exec", "{prompt}"],
        "claude-code": ["claude", "-p", "{prompt}"],
    }


def _canonical_agent_commands(
    commands: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    canonical: Dict[str, List[str]] = {}
    for agent, command in commands.items():
        canonical[canonical_agent_name(agent)] = command
    return canonical


def _has_nonempty_env_prefix(prefix: str) -> bool:
    return any(
        key.startswith(prefix) and value.strip() for key, value in os.environ.items()
    )


@dataclass(frozen=True)
class CoordinatorSettings:
    database_path: str = "data/hermes.db"
    director_api_key: str = ""
    worker_shared_secret: str = ""
    worker_secrets: Dict[str, str] = field(default_factory=dict)
    worker_stale_seconds: int = 45
    worker_eviction_seconds: int = 900
    task_lease_seconds: int = 30
    hmac_max_clock_skew_seconds: int = 300
    default_task_timeout_seconds: int = 900
    max_task_timeout_seconds: int = 7200
    default_max_attempts: int = 2
    maintenance_interval_seconds: float = 5.0
    reconciliation_grace_seconds: int = 3600
    reconciliation_backoff_seconds: int = 5
    default_planner_agent: str = "codex-with-chatgpt"
    planner_lease_seconds: int = 30
    planner_max_attempts: int = 2

    def __post_init__(self) -> None:
        if self.worker_stale_seconds <= 0:
            raise ValueError("worker_stale_seconds must be positive")
        if self.worker_eviction_seconds <= self.worker_stale_seconds:
            raise ValueError(
                "worker_eviction_seconds must be greater than worker_stale_seconds"
            )
        if self.default_planner_agent != "codex-with-chatgpt":
            raise ValueError("Hermes planning must use the codex-with-chatgpt Skill")

    @classmethod
    def from_env(cls) -> "CoordinatorSettings":
        load_env_file()
        return cls(
            database_path=os.getenv("HERMES_DATABASE_PATH", "data/hermes.db"),
            director_api_key=os.getenv("HERMES_DIRECTOR_API_KEY", ""),
            worker_shared_secret=os.getenv("HERMES_WORKER_SHARED_SECRET", ""),
            worker_secrets=_json_dict("HERMES_WORKER_SECRETS_JSON"),
            worker_stale_seconds=_int("HERMES_WORKER_STALE_SECONDS", 45),
            worker_eviction_seconds=_int("HERMES_WORKER_EVICTION_SECONDS", 900),
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
            reconciliation_grace_seconds=_int(
                "HERMES_RECONCILIATION_GRACE_SECONDS", 3600
            ),
            reconciliation_backoff_seconds=_int(
                "HERMES_RECONCILIATION_BACKOFF_SECONDS", 5
            ),
            # Deliberately ignore the old HERMES_PLANNER_DEFAULT_AGENT
            # override: planning must always use the Codex with ChatGPT Skill.
            default_planner_agent="codex-with-chatgpt",
            planner_lease_seconds=_int("HERMES_PLANNER_LEASE_SECONDS", 30),
            planner_max_attempts=_int("HERMES_PLANNER_MAX_ATTEMPTS", 2),
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
    default_agent: str = "default"
    agent_commands: Dict[str, List[str]] = field(default_factory=dict)
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
        agent_commands = _json_agent_commands("HERMES_WORKER_AGENTS_JSON")
        if not agent_commands:
            # Keep the short name accepted by earlier worker deployments.  The
            # JSON shape is the same as HERMES_WORKER_AGENTS_JSON.
            agent_commands = _json_agent_commands("HERMES_AGENT_COMMANDS")
        return cls(
            coordinator_url=os.getenv(
                "HERMES_COORDINATOR_URL", "http://127.0.0.1:8000"
            ).rstrip("/"),
            worker_id=worker_id,
            worker_name=os.getenv("HERMES_WORKER_NAME", hostname),
            shared_secret=os.getenv("HERMES_WORKER_SHARED_SECRET", ""),
            command=command,
            default_agent=os.getenv("HERMES_WORKER_DEFAULT_AGENT", "default"),
            agent_commands=agent_commands,
            allowed_workdir=os.getenv("HERMES_WORKER_ALLOWED_WORKDIR", "."),
            concurrency=_int("HERMES_WORKER_CONCURRENCY", 1),
            task_timeout_seconds=_int("HERMES_WORKER_TASK_TIMEOUT_SECONDS", 900),
            heartbeat_interval_seconds=_int(
                "HERMES_WORKER_HEARTBEAT_INTERVAL_SECONDS", 10
            ),
            poll_interval_seconds=_int("HERMES_WORKER_POLL_INTERVAL_SECONDS", 2),
        )


@dataclass(frozen=True)
class UnifiedWorkerSettings(WorkerSettings):
    """Configuration for the single Worker that serves all execution agents."""

    gateway_url: str = "http://127.0.0.1:8642"
    gateway_id: str = ""
    gateway_kind: str = "local"
    gateway_token: str = ""
    profile_keys: Dict[str, str] = field(default_factory=dict)
    profiles: List[str] = field(default_factory=list)
    default_profile: str = "default"
    gateway_poll_interval_seconds: float = 2.0
    gateway_request_timeout_seconds: float = 30.0
    gateway_stop_wait_seconds: float = 30.0
    gateway_enabled: Optional[bool] = None

    def __post_init__(self) -> None:
        if self.gateway_enabled is None:
            object.__setattr__(
                self,
                "gateway_enabled",
                canonical_agent_name(self.default_agent) == "hermes"
                or bool(self.gateway_id)
                or bool(self.profiles),
            )

    @classmethod
    def from_env(cls) -> "UnifiedWorkerSettings":
        import socket

        load_env_file()
        hostname = socket.gethostname()
        worker_id = os.getenv("HERMES_WORKER_ID", hostname)
        command = shlex.split(
            os.getenv("HERMES_WORKER_COMMAND", "hermes chat -q {prompt}")
        )
        configured_commands = _json_agent_commands("HERMES_WORKER_AGENTS_JSON")
        if not configured_commands:
            configured_commands = _json_agent_commands("HERMES_AGENT_COMMANDS")
        configured_default_agent = os.getenv("HERMES_WORKER_DEFAULT_AGENT")
        gateway_configured = _has_nonempty_env_prefix("HERMES_GATEWAY_")
        legacy_command_mode = (
            configured_default_agent is None and not gateway_configured
        )
        if legacy_command_mode:
            # Before Unified Worker, hermes-worker treated HERMES_WORKER_COMMAND
            # as its default command and did not require a Gateway. Preserve
            # that behavior when an old environment has no new routing signal.
            default_agent = "default"
            agent_commands = _canonical_agent_commands(configured_commands)
            gateway_id = ""
            gateway_enabled = False
        else:
            agent_commands = _default_execution_commands()
            agent_commands.update(_canonical_agent_commands(configured_commands))
            agent_commands["hermes"] = command
            default_agent = canonical_agent_name(configured_default_agent or "hermes")
            gateway_id = os.getenv("HERMES_GATEWAY_ID", hostname)
            gateway_enabled = default_agent == "hermes" or gateway_configured
        profiles = [
            item.strip()
            for item in os.getenv("HERMES_GATEWAY_PROFILES", "").split(",")
            if item.strip()
        ]
        return cls(
            coordinator_url=os.getenv(
                "HERMES_COORDINATOR_URL", "http://127.0.0.1:8000"
            ).rstrip("/"),
            worker_id=worker_id,
            worker_name=os.getenv("HERMES_WORKER_NAME", hostname),
            shared_secret=os.getenv("HERMES_WORKER_SHARED_SECRET", ""),
            command=command,
            default_agent=default_agent,
            agent_commands=agent_commands,
            allowed_workdir=os.getenv("HERMES_WORKER_ALLOWED_WORKDIR", "."),
            concurrency=_int("HERMES_WORKER_CONCURRENCY", 1),
            task_timeout_seconds=_int("HERMES_WORKER_TASK_TIMEOUT_SECONDS", 900),
            heartbeat_interval_seconds=_int(
                "HERMES_WORKER_HEARTBEAT_INTERVAL_SECONDS", 10
            ),
            poll_interval_seconds=_float("HERMES_WORKER_POLL_INTERVAL_SECONDS", 2.0),
            gateway_url=os.getenv("HERMES_GATEWAY_URL", "http://127.0.0.1:8642").rstrip(
                "/"
            ),
            gateway_id=gateway_id,
            gateway_kind=os.getenv("HERMES_GATEWAY_KIND", "local").strip().lower(),
            gateway_token=os.getenv("HERMES_GATEWAY_TOKEN", ""),
            profile_keys=_json_dict("HERMES_GATEWAY_PROFILE_KEYS_JSON"),
            profiles=profiles,
            default_profile=os.getenv(
                "HERMES_GATEWAY_DEFAULT_PROFILE", "default"
            ).strip(),
            gateway_poll_interval_seconds=_float(
                "HERMES_GATEWAY_POLL_INTERVAL_SECONDS", 2.0
            ),
            gateway_request_timeout_seconds=_float(
                "HERMES_GATEWAY_REQUEST_TIMEOUT_SECONDS", 30.0
            ),
            gateway_stop_wait_seconds=_float("HERMES_GATEWAY_STOP_WAIT_SECONDS", 30.0),
            gateway_enabled=gateway_enabled,
        )


@dataclass(frozen=True)
class GatewayWorkerSettings:
    """Configuration for a headless worker backed by one Hermes Gateway."""

    coordinator_url: str = "http://127.0.0.1:8000"
    worker_id: str = ""
    worker_name: str = ""
    shared_secret: str = ""
    gateway_url: str = "http://127.0.0.1:8642"
    gateway_id: str = ""
    gateway_kind: str = "local"
    gateway_token: str = ""
    profile_keys: Dict[str, str] = field(default_factory=dict)
    profiles: List[str] = field(default_factory=list)
    default_profile: str = "default"
    default_agent: str = "hermes"
    concurrency: int = 1
    task_timeout_seconds: int = 900
    heartbeat_interval_seconds: int = 10
    poll_interval_seconds: float = 2.0
    gateway_poll_interval_seconds: float = 2.0
    gateway_request_timeout_seconds: float = 30.0
    gateway_stop_wait_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "GatewayWorkerSettings":
        import socket

        load_env_file()
        hostname = socket.gethostname()
        profiles = [
            p.strip()
            for p in os.getenv("HERMES_GATEWAY_PROFILES", "").split(",")
            if p.strip()
        ]
        return cls(
            coordinator_url=os.getenv(
                "HERMES_COORDINATOR_URL", "http://127.0.0.1:8000"
            ).rstrip("/"),
            worker_id=os.getenv(
                "HERMES_GATEWAY_WORKER_ID", os.getenv("HERMES_WORKER_ID", hostname)
            ),
            worker_name=os.getenv(
                "HERMES_GATEWAY_WORKER_NAME", os.getenv("HERMES_WORKER_NAME", hostname)
            ),
            shared_secret=os.getenv("HERMES_WORKER_SHARED_SECRET", ""),
            gateway_url=os.getenv("HERMES_GATEWAY_URL", "http://127.0.0.1:8642").rstrip(
                "/"
            ),
            gateway_id=os.getenv("HERMES_GATEWAY_ID", hostname),
            gateway_kind=os.getenv("HERMES_GATEWAY_KIND", "local").strip().lower(),
            gateway_token=os.getenv("HERMES_GATEWAY_TOKEN", ""),
            profile_keys=_json_dict("HERMES_GATEWAY_PROFILE_KEYS_JSON"),
            profiles=profiles,
            default_profile=os.getenv(
                "HERMES_GATEWAY_DEFAULT_PROFILE", "default"
            ).strip(),
            default_agent=os.getenv("HERMES_GATEWAY_DEFAULT_AGENT", "hermes"),
            concurrency=_int("HERMES_GATEWAY_WORKER_CONCURRENCY", 1),
            task_timeout_seconds=_int("HERMES_GATEWAY_TASK_TIMEOUT_SECONDS", 900),
            heartbeat_interval_seconds=_int(
                "HERMES_GATEWAY_HEARTBEAT_INTERVAL_SECONDS", 10
            ),
            poll_interval_seconds=_float(
                "HERMES_GATEWAY_WORKER_POLL_INTERVAL_SECONDS", 2.0
            ),
            gateway_poll_interval_seconds=_float(
                "HERMES_GATEWAY_POLL_INTERVAL_SECONDS", 2.0
            ),
            gateway_request_timeout_seconds=_float(
                "HERMES_GATEWAY_REQUEST_TIMEOUT_SECONDS", 30.0
            ),
            gateway_stop_wait_seconds=_float("HERMES_GATEWAY_STOP_WAIT_SECONDS", 30.0),
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
