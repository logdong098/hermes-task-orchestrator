from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class TaskStatus(str, Enum):
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


class HeartbeatRequest(BaseModel):
    running_task_ids: List[str] = Field(default_factory=list, max_length=128)


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

    @field_validator("prompt")
    @classmethod
    def prompt_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prompt must not be blank")
        return value


class TaskStatusUpdate(BaseModel):
    status: TaskStatus


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


class NotificationResponse(BaseModel):
    id: int
    task_id: str
    channel: str
    destination: str
    payload: Dict[str, Any]
    created_at: float
