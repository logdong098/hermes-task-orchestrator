from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import TERMINAL_STATUSES, TaskStatus


class ConflictError(RuntimeError):
    pass


class NotFoundError(RuntimeError):
    pass


class SQLiteStore:
    """SQLite-backed repository; API code only depends on these repository methods."""

    def __init__(self, database_path: str) -> None:
        if database_path == ":memory:":
            raise ValueError(
                "SQLiteStore does not support :memory: because operations use short connections"
            )
        self.database_path = database_path

    def initialize(self) -> None:
        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    max_concurrency INTEGER NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    registered_at REAL NOT NULL,
                    last_heartbeat_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL,
                    target_worker_id TEXT,
                    claimed_by TEXT,
                    workdir TEXT,
                    timeout_seconds INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    priority INTEGER NOT NULL DEFAULT 0,
                    creator_user_id TEXT,
                    telegram_chat_id TEXT,
                    idempotency_key TEXT,
                    result TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    lease_expires_at REAL,
                    FOREIGN KEY (claimed_by) REFERENCES workers(worker_id)
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_claim
                    ON tasks(status, target_worker_id, priority DESC, created_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_worker
                    ON tasks(claimed_by, status);

                CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    acknowledged_at REAL,
                    UNIQUE(task_id, channel),
                    FOREIGN KEY (task_id) REFERENCES tasks(id)
                );

                CREATE TABLE IF NOT EXISTS worker_nonces (
                    worker_id TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    used_at REAL NOT NULL,
                    PRIMARY KEY (worker_id, nonce)
                );
                CREATE INDEX IF NOT EXISTS idx_worker_nonces_used_at
                    ON worker_nonces(used_at);
                """
            )
            task_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(tasks)")
            }
            if "idempotency_key" not in task_columns:
                connection.execute("ALTER TABLE tasks ADD COLUMN idempotency_key TEXT")
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency_key
                ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=10,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _task(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    @staticmethod
    def _worker(row: sqlite3.Row, stale_seconds: int, now: float) -> Dict[str, Any]:
        data = dict(row)
        data["capabilities"] = json.loads(data.pop("capabilities_json"))
        data["metadata"] = json.loads(data.pop("metadata_json"))
        data["status"] = (
            "online" if now - data["last_heartbeat_at"] <= stale_seconds else "offline"
        )
        return data

    def register_worker(
        self, worker: Dict[str, Any], now: Optional[float] = None
    ) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO workers (
                    worker_id, name, max_concurrency, capabilities_json,
                    metadata_json, registered_at, last_heartbeat_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    name = excluded.name,
                    max_concurrency = excluded.max_concurrency,
                    capabilities_json = excluded.capabilities_json,
                    metadata_json = excluded.metadata_json,
                    last_heartbeat_at = excluded.last_heartbeat_at
                """,
                (
                    worker["worker_id"],
                    worker["name"],
                    worker["max_concurrency"],
                    self._json(worker["capabilities"]),
                    self._json(worker["metadata"]),
                    current,
                    current,
                ),
            )
            row = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?", (worker["worker_id"],)
            ).fetchone()
        return self._worker(row, stale_seconds=0, now=current)

    def consume_worker_nonce(
        self,
        worker_id: str,
        nonce: str,
        retention_seconds: int,
        now: Optional[float] = None,
    ) -> bool:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM worker_nonces WHERE used_at < ?",
                (current - retention_seconds,),
            )
            try:
                connection.execute(
                    "INSERT INTO worker_nonces (worker_id, nonce, used_at) VALUES (?, ?, ?)",
                    (worker_id, nonce, current),
                )
            except sqlite3.IntegrityError:
                connection.rollback()
                return False
            connection.commit()
        return True

    def heartbeat(
        self,
        worker_id: str,
        running_task_ids: List[str],
        lease_seconds: int,
        now: Optional[float] = None,
    ) -> List[str]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._maintenance(connection, current)
            cursor = connection.execute(
                "UPDATE workers SET last_heartbeat_at = ? WHERE worker_id = ?",
                (current, worker_id),
            )
            if cursor.rowcount == 0:
                connection.rollback()
                raise NotFoundError("worker not registered")
            if running_task_ids:
                placeholders = ",".join("?" for _ in running_task_ids)
                connection.execute(
                    f"""
                    UPDATE tasks SET lease_expires_at = ?, updated_at = ?
                    WHERE claimed_by = ?
                      AND id IN ({placeholders})
                      AND status IN (?, ?)
                    """,
                    (
                        current + lease_seconds,
                        current,
                        worker_id,
                        *running_task_ids,
                        TaskStatus.CLAIMED.value,
                        TaskStatus.RUNNING.value,
                    ),
                )
            rows = connection.execute(
                """
                SELECT id FROM tasks
                WHERE claimed_by = ? AND status = ?
                """,
                (worker_id, TaskStatus.CANCEL_REQUESTED.value),
            ).fetchall()
            connection.commit()
        return [row["id"] for row in rows]

    def list_workers(
        self, stale_seconds: int, now: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM workers ORDER BY worker_id"
            ).fetchall()
        return [self._worker(row, stale_seconds, current) for row in rows]

    def create_task(
        self,
        task: Dict[str, Any],
        default_timeout_seconds: int,
        max_timeout_seconds: int,
        default_max_attempts: int,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        timeout_seconds = min(
            task.get("timeout_seconds") or default_timeout_seconds,
            max_timeout_seconds,
        )
        task_id = str(uuid.uuid4())
        values = (
            task_id,
            task["prompt"],
            TaskStatus.PENDING.value,
            task.get("target_worker_id"),
            task.get("workdir"),
            timeout_seconds,
            task.get("max_attempts") or default_max_attempts,
            task.get("priority", 0),
            task.get("creator_user_id"),
            task.get("telegram_chat_id"),
            task.get("idempotency_key"),
            current,
            current,
        )
        with self._connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO tasks (
                        id, prompt, status, target_worker_id, workdir,
                        timeout_seconds, max_attempts, priority, creator_user_id,
                        telegram_chat_id, idempotency_key, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
            except sqlite3.IntegrityError:
                idempotency_key = task.get("idempotency_key")
                if not idempotency_key:
                    raise
                existing = connection.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing is None:
                    raise
                return self._task(existing)
            row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return self._task(row)

    def get_task(self, task_id: str) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("task not found")
        return self._task(row)

    def list_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._task(row) for row in rows]

    def claim_task(
        self,
        worker_id: str,
        lease_seconds: int,
        now: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._maintenance(connection, current)
            worker = connection.execute(
                "SELECT max_concurrency FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            if worker is None:
                connection.rollback()
                raise NotFoundError("worker not registered")
            active_count = connection.execute(
                """
                SELECT COUNT(*) AS count FROM tasks
                WHERE claimed_by = ? AND status IN (?, ?, ?)
                """,
                (
                    worker_id,
                    TaskStatus.CLAIMED.value,
                    TaskStatus.RUNNING.value,
                    TaskStatus.CANCEL_REQUESTED.value,
                ),
            ).fetchone()["count"]
            if active_count >= worker["max_concurrency"]:
                connection.commit()
                return None
            row = connection.execute(
                """
                SELECT * FROM tasks
                WHERE status = ?
                  AND (target_worker_id IS NULL OR target_worker_id = ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (TaskStatus.PENDING.value, worker_id),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?, claimed_by = ?, attempt_count = attempt_count + 1,
                    lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    TaskStatus.CLAIMED.value,
                    worker_id,
                    current + lease_seconds,
                    current,
                    row["id"],
                    TaskStatus.PENDING.value,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                return None
            claimed = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (row["id"],)
            ).fetchone()
            connection.commit()
        return self._task(claimed)

    def update_task_status(
        self,
        task_id: str,
        worker_id: str,
        status: str,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._maintenance(connection, current)
            row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                connection.rollback()
                raise NotFoundError("task not found")
            if row["claimed_by"] != worker_id:
                connection.rollback()
                raise ConflictError("task belongs to another worker")
            if (
                status != TaskStatus.RUNNING.value
                or row["status"] != TaskStatus.CLAIMED.value
            ):
                connection.rollback()
                raise ConflictError(f"invalid transition: {row['status']} -> {status}")
            connection.execute(
                """
                UPDATE tasks SET status = ?, started_at = COALESCE(started_at, ?), updated_at = ?
                WHERE id = ?
                """,
                (status, current, current, task_id),
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            connection.commit()
        return self._task(updated)

    def report_result(
        self,
        task_id: str,
        worker_id: str,
        status: str,
        result: Optional[str],
        error: Optional[str],
        retryable: bool,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        allowed = {
            TaskStatus.SUCCEEDED.value,
            TaskStatus.FAILED.value,
            TaskStatus.CANCELLED.value,
            TaskStatus.TIMED_OUT.value,
        }
        if status not in allowed:
            raise ConflictError("result status must be terminal")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._maintenance(connection, current)
            row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                connection.rollback()
                raise NotFoundError("task not found")
            if row["claimed_by"] != worker_id:
                connection.rollback()
                raise ConflictError("task belongs to another worker")
            if row["status"] in TERMINAL_STATUSES:
                connection.commit()
                return self._task(row)
            if row["status"] == TaskStatus.CANCEL_REQUESTED.value:
                if status != TaskStatus.CANCELLED.value:
                    connection.rollback()
                    raise ConflictError(
                        f"invalid transition: {row['status']} -> {status}"
                    )
            elif row["status"] != TaskStatus.RUNNING.value:
                connection.rollback()
                raise ConflictError(f"invalid transition: {row['status']} -> {status}")
            if row["status"] == TaskStatus.CANCEL_REQUESTED.value:
                status = TaskStatus.CANCELLED.value
                retryable = False
            if (
                status == TaskStatus.FAILED.value
                and retryable
                and row["attempt_count"] < row["max_attempts"]
            ):
                connection.execute(
                    """
                    UPDATE tasks SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                        updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (TaskStatus.PENDING.value, current, error, task_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE tasks SET status = ?, result = ?, error = ?, finished_at = ?,
                        lease_expires_at = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (status, result, error, current, current, task_id),
                )
                final_row = connection.execute(
                    "SELECT * FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                self._enqueue_notification(connection, final_row, current)
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            connection.commit()
        return self._task(updated)

    def cancel_task(self, task_id: str, now: Optional[float] = None) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                connection.rollback()
                raise NotFoundError("task not found")
            if row["status"] in TERMINAL_STATUSES:
                connection.commit()
                return self._task(row)
            if row["status"] == TaskStatus.PENDING.value:
                next_status = TaskStatus.CANCELLED.value
                finished_at = current
            else:
                next_status = TaskStatus.CANCEL_REQUESTED.value
                finished_at = None
            connection.execute(
                """
                UPDATE tasks SET status = ?, updated_at = ?, finished_at = ?
                WHERE id = ?
                """,
                (next_status, current, finished_at, task_id),
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if next_status == TaskStatus.CANCELLED.value:
                self._enqueue_notification(connection, updated, current)
            connection.commit()
        return self._task(updated)

    def list_notifications(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM notifications
                WHERE acknowledged_at IS NULL
                ORDER BY id ASC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "channel": row["channel"],
                "destination": row["destination"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def acknowledge_notification(
        self, notification_id: int, now: Optional[float] = None
    ) -> None:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE notifications SET acknowledged_at = ?
                WHERE id = ? AND acknowledged_at IS NULL
                """,
                (current, notification_id),
            )
        if cursor.rowcount == 0:
            raise NotFoundError("notification not found or already acknowledged")

    def run_maintenance(self, now: Optional[float] = None) -> None:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._maintenance(connection, current)
            connection.commit()

    def _maintenance(self, connection: sqlite3.Connection, now: float) -> None:
        rows = connection.execute(
            """
            SELECT * FROM tasks
            WHERE status IN (?, ?, ?) AND (
                (started_at IS NOT NULL AND started_at + timeout_seconds < ?)
                OR (lease_expires_at IS NOT NULL AND lease_expires_at < ?)
            )
            """,
            (
                TaskStatus.CLAIMED.value,
                TaskStatus.RUNNING.value,
                TaskStatus.CANCEL_REQUESTED.value,
                now,
                now,
            ),
        ).fetchall()
        for row in rows:
            if row["status"] == TaskStatus.CANCEL_REQUESTED.value:
                next_status = TaskStatus.CANCELLED.value
            elif (
                row["started_at"] is not None
                and row["started_at"] + row["timeout_seconds"] < now
            ):
                next_status = TaskStatus.TIMED_OUT.value
            elif row["attempt_count"] < row["max_attempts"]:
                connection.execute(
                    """
                    UPDATE tasks SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                        updated_at = ?, error = ? WHERE id = ?
                    """,
                    (
                        TaskStatus.PENDING.value,
                        now,
                        "worker lease expired; task requeued",
                        row["id"],
                    ),
                )
                continue
            else:
                next_status = TaskStatus.TIMED_OUT.value
            connection.execute(
                """
                UPDATE tasks SET status = ?, lease_expires_at = NULL, updated_at = ?,
                    finished_at = ?, error = COALESCE(error, ?)
                WHERE id = ?
                """,
                (next_status, now, now, "task lease or execution timeout", row["id"]),
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (row["id"],)
            ).fetchone()
            self._enqueue_notification(connection, updated, now)

    def _enqueue_notification(
        self, connection: sqlite3.Connection, task: sqlite3.Row, now: float
    ) -> None:
        if not task["telegram_chat_id"] or task["status"] not in TERMINAL_STATUSES:
            return
        payload = {
            "task_id": task["id"],
            "status": task["status"],
            "result": task["result"],
            "error": task["error"],
        }
        connection.execute(
            """
            INSERT OR IGNORE INTO notifications (
                task_id, channel, destination, payload_json, created_at
            ) VALUES (?, 'telegram', ?, ?, ?)
            """,
            (
                task["id"],
                task["telegram_chat_id"],
                self._json(payload),
                now,
            ),
        )
