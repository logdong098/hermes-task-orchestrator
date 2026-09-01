from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class WorkerKind(str, Enum):
    COMMAND = "command"
    GATEWAY = "gateway"


class GatewayKind(str, Enum):
    LOCAL = "local"
    REMOTE = "remote"
    SSH = "ssh"
    CLOUD = "cloud"


class TaskStatus(str, Enum):
    PLANNING_PENDING = "planning_pending"
    PLANNING = "planning"
    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


TERMINAL_STATUSES = {
    TaskStatus.CANCELLED.value,
    TaskStatus.SUCCEEDED.value,
    TaskStatus.FAILED.value,
    TaskStatus.TIMED_OUT.value,
}


class WorkerRegistration(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=128)
    max_concurrency: int = Field(default=1, ge=1, le=64)
    capabilities: List[str] = Field(default_factory=lambda: ["hermes-chat"])
    metadata: Dict[str, Any] = Field(default_factory=dict)
    worker_kind: WorkerKind = WorkerKind.COMMAND
    default_agent: str = Field(default="default", min_length=1, max_length=128)
    routes: List["WorkerRoute"] = Field(default_factory=list, max_length=128)

    def model_post_init(self, __context: Any) -> None:
        if self.worker_kind == WorkerKind.GATEWAY:
            if not self.routes:
                raise ValueError("gateway workers must register at least one route")
            if any(not route.gateway_id or not route.profile for route in self.routes):
                raise ValueError(
                    "gateway routes require non-blank gateway_id and profile"
                )
        elif self.routes:
            raise ValueError("command workers cannot register gateway routes")


class WorkerRoute(BaseModel):
    route_id: str = Field(min_length=1, max_length=256)
    gateway_id: Optional[str] = Field(default=None, max_length=256)
    profile: Optional[str] = Field(default=None, max_length=256)
    target_profile: Optional[str] = Field(default=None, max_length=256)
    gateway_kind: GatewayKind = GatewayKind.LOCAL
    supported_agents: List[str] = Field(default_factory=list, max_length=128)
    default_agent: str = Field(default="default", min_length=1, max_length=128)
    labels: Dict[str, str] = Field(default_factory=dict)

    @field_validator("gateway_id", "profile", "target_profile")
    @classmethod
    def normalize_route_text(cls, value: Optional[str]) -> Optional[str]:
        return value.strip() if value is not None else None


class HeartbeatRequest(BaseModel):
    running_task_ids: List[str] = Field(default_factory=list, max_length=128)
    routes: Optional[List[WorkerRoute]] = Field(default=None, max_length=128)

    def model_post_init(self, __context: Any) -> None:
        if self.routes is not None and any(
            not route.gateway_id or not route.profile for route in self.routes
        ):
            raise ValueError("heartbeat gateway routes require gateway_id and profile")


class PlannerAgent(str, Enum):
    CLAUDE = "claude"
    CODEX = "codex"


class TaskCreate(BaseModel):
    prompt: str = Field(min_length=1, max_length=100_000)
    target_worker_id: Optional[str] = Field(default=None, max_length=128)
    workdir: Optional[str] = Field(default=None, max_length=1024)
    timeout_seconds: Optional[int] = Field(default=None, ge=1)
    max_attempts: Optional[int] = Field(default=None, ge=1, le=10)
    priority: int = Field(default=0, ge=-100, le=100)
    creator_user_id: Optional[str] = Field(default=None, max_length=128)
    telegram_chat_id: Optional[str] = Field(default=None, max_length=128)
    idempotency_key: Optional[str] = Field(default=None, max_length=256)
    planner_agent: Optional[PlannerAgent] = None
    execution_agent: Optional[str] = Field(default=None, min_length=1, max_length=128)
    target_gateway_id: Optional[str] = Field(default=None, max_length=256)
    target_profile: Optional[str] = Field(default=None, max_length=256)

    @field_validator("prompt")
    @classmethod
    def prompt_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prompt must not be blank")
        return value

    @field_validator("execution_agent")
    @classmethod
    def normalize_execution_agent(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("execution_agent must not be blank")
        if not all(
            character.isalnum() or character in "._-" for character in normalized
        ):
            raise ValueError("execution_agent contains unsupported characters")
        return normalized

    @field_validator("target_gateway_id", "target_profile")
    @classmethod
    def normalize_target_route(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("target route values must not be blank")
        return value

    def model_post_init(self, __context: Any) -> None:
        if (self.target_gateway_id is None) != (self.target_profile is None):
            raise ValueError(
                "target_gateway_id and target_profile must be provided together"
            )


class TaskStatusUpdate(BaseModel):
    status: TaskStatus


class TaskRemoteRunUpdate(BaseModel):
    remote_run_id: str = Field(min_length=1, max_length=256)
    remote_session_id: Optional[str] = Field(default=None, max_length=256)


class TaskResult(BaseModel):
    status: TaskStatus
    result: Optional[str] = Field(default=None, max_length=2_000_000)
    error: Optional[str] = Field(default=None, max_length=100_000)
    retryable: bool = False


class TaskResponse(BaseModel):
    id: str
    prompt: str
    status: TaskStatus
    target_worker_id: Optional[str]
    claimed_by: Optional[str]
    workdir: Optional[str]
    timeout_seconds: int
    max_attempts: int
    attempt_count: int
    priority: int
    creator_user_id: Optional[str]
    telegram_chat_id: Optional[str]
    idempotency_key: Optional[str] = None
    planner_agent: Optional[PlannerAgent] = None
    execution_agent: Optional[str] = None
    target_gateway_id: Optional[str] = None
    target_profile: Optional[str] = None
    resolved_worker_id: Optional[str] = None
    resolved_route_id: Optional[str] = None
    resolved_gateway_id: Optional[str] = None
    resolved_profile: Optional[str] = None
    resolved_execution_agent: Optional[str] = None
    remote_run_id: Optional[str] = None
    remote_session_id: Optional[str] = None
    routing_diagnostic: Optional[str] = None
    plan: Optional[str] = None
    execution_prompt: Optional[str] = None
    planner_attempt_count: int = 0
    planner_max_attempts: int = 2
    planner_started_at: Optional[float] = None
    planner_finished_at: Optional[float] = None
    planner_lease_expires_at: Optional[float] = None
    result: Optional[str]
    error: Optional[str]
    created_at: float
    updated_at: float
    started_at: Optional[float]
    finished_at: Optional[float]
    lease_expires_at: Optional[float]


class WorkerResponse(BaseModel):
    worker_id: str
    name: str
    max_concurrency: int
    capabilities: List[str]
    metadata: Dict[str, Any]
    registered_at: float
    last_heartbeat_at: float
    status: str
    worker_kind: WorkerKind = WorkerKind.COMMAND
    default_agent: str = "default"
    routes: List[WorkerRoute] = Field(default_factory=list)


class WorkerRouteResponse(WorkerRoute):
    worker_id: str
    worker_kind: WorkerKind
    last_seen_at: float
    status: str
    availability_reason: Optional[str] = None


class NotificationResponse(BaseModel):
    id: int
    task_id: str
    channel: str
    destination: str
    payload: Dict[str, Any]
    created_at: float
