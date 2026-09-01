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

                CREATE TABLE IF NOT EXISTS worker_routes (
                    route_id TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    gateway_id TEXT,
                    profile TEXT,
                    target_profile TEXT,
                    gateway_kind TEXT NOT NULL,
                    supported_agents_json TEXT NOT NULL,
                    default_agent TEXT NOT NULL,
                    labels_json TEXT NOT NULL,
                    last_seen_at REAL NOT NULL,
                    UNIQUE(worker_id, gateway_id, profile),
                    FOREIGN KEY (worker_id) REFERENCES workers(worker_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_worker_routes_lookup
                    ON worker_routes(gateway_id, profile, worker_id);

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
                    planner_agent TEXT,
                    execution_agent TEXT,
                    plan TEXT,
                    execution_prompt TEXT,
                    planner_attempt_count INTEGER NOT NULL DEFAULT 0,
                    planner_max_attempts INTEGER NOT NULL DEFAULT 2,
                    planner_started_at REAL,
                    planner_finished_at REAL,
                    planner_lease_expires_at REAL,
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
            worker_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(workers)")
            }
            for column, definition in {
                "worker_kind": "TEXT NOT NULL DEFAULT 'command'",
                "default_agent": "TEXT NOT NULL DEFAULT 'default'",
            }.items():
                if column not in worker_columns:
                    connection.execute(
                        f"ALTER TABLE workers ADD COLUMN {column} {definition}"
                    )
            task_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(tasks)")
            }
            additive_route_columns = {
                "target_gateway_id": "TEXT",
                "target_profile": "TEXT",
                "resolved_worker_id": "TEXT",
                "resolved_route_id": "TEXT",
                "resolved_gateway_id": "TEXT",
                "resolved_profile": "TEXT",
                "resolved_execution_agent": "TEXT",
                "remote_run_id": "TEXT",
                "remote_session_id": "TEXT",
            }
            for column, definition in additive_route_columns.items():
                if column not in task_columns:
                    connection.execute(
                        f"ALTER TABLE tasks ADD COLUMN {column} {definition}"
                    )
            if "idempotency_key" not in task_columns:
                connection.execute("ALTER TABLE tasks ADD COLUMN idempotency_key TEXT")
            additive_task_columns = {
                "planner_agent": "TEXT",
                "execution_agent": "TEXT",
                "plan": "TEXT",
                "execution_prompt": "TEXT",
                "planner_attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "planner_max_attempts": "INTEGER NOT NULL DEFAULT 2",
                "planner_started_at": "REAL",
                "planner_finished_at": "REAL",
                "planner_lease_expires_at": "REAL",
            }
            for column, definition in additive_task_columns.items():
                if column not in task_columns:
                    connection.execute(
                        f"ALTER TABLE tasks ADD COLUMN {column} {definition}"
                    )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency_key
                ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL
                """
            )
            legacy_command_workers = connection.execute(
                """
                SELECT * FROM workers w
                WHERE worker_kind = 'command'
                  AND NOT EXISTS (
                    SELECT 1 FROM worker_routes r WHERE r.worker_id = w.worker_id
                  )
                """
            ).fetchall()
            for worker in legacy_command_workers:
                capabilities = json.loads(worker["capabilities_json"])
                supported_agents = [
                    item[6:]
                    for item in capabilities
                    if isinstance(item, str) and item.startswith("agent:")
                ]
                metadata = json.loads(worker["metadata_json"])
                default_agent = worker["default_agent"]
                if default_agent == "default" and metadata.get("default_agent"):
                    default_agent = metadata["default_agent"]
                connection.execute(
                    """
                    INSERT OR IGNORE INTO worker_routes (
                        route_id, worker_id, gateway_id, profile, target_profile,
                        gateway_kind, supported_agents_json, default_agent,
                        labels_json, last_seen_at
                    ) VALUES (?, ?, NULL, NULL, NULL, 'local', ?, ?, '{}', ?)
                    """,
                    (
                        f"{worker['worker_id']}:command",
                        worker["worker_id"],
                        self._json(supported_agents),
                        default_agent,
                        worker["last_heartbeat_at"],
                    ),
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

    @staticmethod
    def _route(row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["supported_agents"] = json.loads(data.pop("supported_agents_json"))
        data["labels"] = json.loads(data.pop("labels_json"))
        return data

    def register_worker(
        self, worker: Dict[str, Any], now: Optional[float] = None
    ) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        if worker.get("worker_kind", "command") == "gateway":
            routes = worker.get("routes") or []
            if not routes:
                raise ConflictError("gateway workers must register at least one route")
            for route in routes:
                data = route.model_dump() if hasattr(route, "model_dump") else route
                if not data.get("gateway_id") or not data.get("profile"):
                    raise ConflictError(
                        "gateway routes require non-blank gateway_id and profile"
                    )
        elif worker.get("routes"):
            raise ConflictError("command workers cannot register gateway routes")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO workers (
                    worker_id, name, max_concurrency, capabilities_json,
                    metadata_json, registered_at, last_heartbeat_at, worker_kind,
                    default_agent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    name = excluded.name,
                    max_concurrency = excluded.max_concurrency,
                    capabilities_json = excluded.capabilities_json,
                    metadata_json = excluded.metadata_json,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    worker_kind = excluded.worker_kind,
                    default_agent = excluded.default_agent
                """,
                (
                    worker["worker_id"],
                    worker["name"],
                    worker["max_concurrency"],
                    self._json(worker["capabilities"]),
                    self._json(worker["metadata"]),
                    current,
                    current,
                    worker.get("worker_kind", "command"),
                    (
                        worker.get("metadata", {}).get("default_agent")
                        if worker.get("default_agent", "default") == "default"
                        and worker.get("metadata", {}).get("default_agent")
                        else worker.get("default_agent", "default")
                    ),
                ),
            )
            connection.execute(
                "DELETE FROM worker_routes WHERE worker_id = ?", (worker["worker_id"],)
            )
            routes = worker.get("routes") or []
            if not routes and worker.get("worker_kind", "command") == "command":
                routes = [
                    {
                        "route_id": f"{worker['worker_id']}:command",
                        "gateway_id": None,
                        "profile": None,
                        "target_profile": None,
                        "gateway_kind": "local",
                        "supported_agents": [
                            item[6:]
                            for item in worker["capabilities"]
                            if item.startswith("agent:")
                        ],
                        "default_agent": (
                            worker.get("metadata", {}).get("default_agent")
                            if worker.get("default_agent", "default") == "default"
                            and worker.get("metadata", {}).get("default_agent")
                            else worker.get("default_agent", "default")
                        ),
                        "labels": {},
                    }
                ]
            for route in routes:
                route = route.model_dump() if hasattr(route, "model_dump") else route
                connection.execute(
                    """INSERT INTO worker_routes
                    (route_id, worker_id, gateway_id, profile, target_profile,
                     gateway_kind, supported_agents_json, default_agent, labels_json, last_seen_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        route["route_id"],
                        worker["worker_id"],
                        route.get("gateway_id"),
                        route.get("profile"),
                        route.get("target_profile"),
                        route.get("gateway_kind", "local"),
                        self._json(route.get("supported_agents", [])),
                        route.get("default_agent", "default"),
                        self._json(route.get("labels", {})),
                        current,
                    ),
                )
            row = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?", (worker["worker_id"],)
            ).fetchone()
            route_rows = connection.execute(
                "SELECT * FROM worker_routes WHERE worker_id = ? ORDER BY route_id",
                (worker["worker_id"],),
            ).fetchall()
        registered = self._worker(row, stale_seconds=0, now=current)
        registered["routes"] = [self._route(route) for route in route_rows]
        return registered

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
        routes: Optional[List[Dict[str, Any]]] = None,
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
            if routes is not None:
                connection.execute(
                    "DELETE FROM worker_routes WHERE worker_id = ?", (worker_id,)
                )
                for route in routes:
                    route = (
                        route.model_dump() if hasattr(route, "model_dump") else route
                    )
                    connection.execute(
                        """INSERT INTO worker_routes
                        (route_id, worker_id, gateway_id, profile, target_profile, gateway_kind,
                         supported_agents_json, default_agent, labels_json, last_seen_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            route["route_id"],
                            worker_id,
                            route.get("gateway_id"),
                            route.get("profile"),
                            route.get("target_profile"),
                            route.get("gateway_kind", "local"),
                            self._json(route.get("supported_agents", [])),
                            route.get("default_agent", "default"),
                            self._json(route.get("labels", {})),
                            current,
                        ),
                    )
            else:
                connection.execute(
                    "UPDATE worker_routes SET last_seen_at = ? WHERE worker_id = ?",
                    (current, worker_id),
                )
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
            route_rows = connection.execute(
                "SELECT * FROM worker_routes ORDER BY worker_id, route_id"
            ).fetchall()
        routes_by_worker: Dict[str, List[Dict[str, Any]]] = {}
        for route in route_rows:
            routes_by_worker.setdefault(route["worker_id"], []).append(
                self._route(route)
            )
        workers = []
        for row in rows:
            worker = self._worker(row, stale_seconds, current)
            worker["routes"] = routes_by_worker.get(row["worker_id"], [])
            workers.append(worker)
        return workers

    def list_routes(
        self, stale_seconds: int = 45, now: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            rows = connection.execute("""SELECT r.*, w.worker_kind FROM worker_routes r
                JOIN workers w ON w.worker_id = r.worker_id ORDER BY r.route_id""").fetchall()
        result = []
        for row in rows:
            item = self._route(row)
            item["status"] = (
                "online"
                if current - item["last_seen_at"] <= stale_seconds
                else "offline"
            )
            item["availability_reason"] = (
                None if item["status"] == "online" else "worker heartbeat is stale"
            )
            result.append(item)
        return result

    def create_task(
        self,
        task: Dict[str, Any],
        default_timeout_seconds: int,
        max_timeout_seconds: int,
        default_max_attempts: int,
        default_planner_max_attempts: int = 2,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        current = now if now is not None else time.time()
        timeout_seconds = min(
            task.get("timeout_seconds") or default_timeout_seconds,
            max_timeout_seconds,
        )
        task_id = str(uuid.uuid4())
        planner_agent = task.get("planner_agent")
        values = (
            task_id,
            task["prompt"],
            (
                TaskStatus.PLANNING_PENDING.value
                if planner_agent
                else TaskStatus.PENDING.value
            ),
            task.get("target_worker_id"),
            task.get("target_gateway_id"),
            task.get("target_profile"),
            task.get("workdir"),
            timeout_seconds,
            task.get("max_attempts") or default_max_attempts,
            task.get("priority", 0),
            task.get("creator_user_id"),
            task.get("telegram_chat_id"),
            task.get("idempotency_key"),
            planner_agent,
            task.get("execution_agent"),
            default_planner_max_attempts,
            current,
            current,
        )
        with self._connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO tasks (
                        id, prompt, status, target_worker_id, target_gateway_id,
                        target_profile, workdir,
                        timeout_seconds, max_attempts, priority, creator_user_id,
                        telegram_chat_id, idempotency_key, planner_agent,
                        execution_agent, planner_max_attempts, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                "SELECT max_concurrency, capabilities_json, worker_kind, default_agent FROM workers WHERE worker_id = ?",
                (worker_id,),
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
            rows = connection.execute(
                """
                SELECT * FROM tasks
                WHERE status = ?
                  AND (target_worker_id IS NULL OR target_worker_id = ?)
                  AND (target_gateway_id IS NOT NULL OR ? = 'command')
                ORDER BY priority DESC, created_at ASC
                """,
                (TaskStatus.PENDING.value, worker_id, worker["worker_kind"]),
            ).fetchall()
            capabilities = set(json.loads(worker["capabilities_json"]))
            route_rows = connection.execute(
                "SELECT * FROM worker_routes WHERE worker_id = ? ORDER BY route_id",
                (worker_id,),
            ).fetchall()
            route_by_key = {
                (r["gateway_id"], r["target_profile"] or r["profile"]): r
                for r in route_rows
            }
            selected = None
            for candidate in rows:
                route = None
                if candidate["target_gateway_id"] is not None:
                    route = route_by_key.get(
                        (candidate["target_gateway_id"], candidate["target_profile"])
                    )
                    if route is None:
                        continue
                if candidate["execution_agent"] is not None:
                    agent = candidate["execution_agent"]
                    if route:
                        supported = set(json.loads(route["supported_agents_json"]))
                        if agent not in supported and f"agent:{agent}" not in supported:
                            continue
                    elif (
                        agent not in capabilities
                        and f"agent:{agent}" not in capabilities
                    ):
                        continue
                selected = (candidate, route)
                break
            row, route = selected if selected else (None, None)
            if row is None:
                connection.commit()
                return None
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?, claimed_by = ?, attempt_count = attempt_count + 1,
                    lease_expires_at = ?, updated_at = ?, resolved_worker_id = ?,
                    resolved_route_id = ?, resolved_gateway_id = ?, resolved_profile = ?,
                    resolved_execution_agent = ?
                WHERE id = ? AND status = ?
                """,
                (
                    TaskStatus.CLAIMED.value,
                    worker_id,
                    current + lease_seconds,
                    current,
                    worker_id,
                    route["route_id"] if route else f"{worker_id}:command",
                    route["gateway_id"] if route else None,
                    route["profile"] if route else None,
                    row["execution_agent"]
                    or (route["default_agent"] if route else worker["default_agent"]),
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

    def attach_remote_run(
        self,
        task_id: str,
        worker_id: str,
        remote_run_id: str,
        remote_session_id: Optional[str] = None,
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
            if row["status"] not in (
                TaskStatus.CLAIMED.value,
                TaskStatus.RUNNING.value,
            ):
                connection.commit()
                raise ConflictError("remote run can only be attached to an active task")
            if row["claimed_by"] != worker_id:
                connection.rollback()
                raise ConflictError("task belongs to another worker")
            if row["lease_expires_at"] is None or row["lease_expires_at"] <= current:
                connection.rollback()
                raise ConflictError("task lease expired before remote run attachment")
            connection.execute(
                """UPDATE tasks SET remote_run_id = ?, remote_session_id = ?, updated_at = ?
                WHERE id = ? AND claimed_by = ? AND status IN (?, ?)""",
                (
                    remote_run_id,
                    remote_session_id,
                    current,
                    task_id,
                    worker_id,
                    TaskStatus.CLAIMED.value,
                    TaskStatus.RUNNING.value,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            connection.commit()
        return self._task(updated)

    def claim_planning_task(
        self,
        lease_seconds: int,
        task_id: Optional[str] = None,
        now: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """Atomically lease the next Coordinator-local planning task."""
        current = now if now is not None else time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._maintenance(connection, current)
            if task_id is None:
                row = connection.execute(
                    """
                    SELECT * FROM tasks
                    WHERE status = ?
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                    """,
                    (TaskStatus.PLANNING_PENDING.value,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM tasks WHERE id = ? AND status = ?",
                    (task_id, TaskStatus.PLANNING_PENDING.value),
                ).fetchone()
            if row is None:
                connection.commit()
                return None
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?, planner_attempt_count = planner_attempt_count + 1,
                    planner_started_at = COALESCE(planner_started_at, ?),
                    planner_lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    TaskStatus.PLANNING.value,
                    current,
                    current + lease_seconds,
                    current,
                    row["id"],
                    TaskStatus.PLANNING_PENDING.value,
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

    def extend_planning_lease(
        self,
        task_id: str,
        lease_seconds: int,
        now: Optional[float] = None,
        expected_attempt_count: Optional[int] = None,
    ) -> bool:
        current = now if now is not None else time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tasks SET planner_lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                  AND planner_lease_expires_at >= ?
                  AND (? IS NULL OR planner_attempt_count = ?)
                """,
                (
                    current + lease_seconds,
                    current,
                    task_id,
                    TaskStatus.PLANNING.value,
                    current,
                    expected_attempt_count,
                    expected_attempt_count,
                ),
            )
        return cursor.rowcount == 1

    def complete_planning(
        self,
        task_id: str,
        plan: str,
        execution_prompt: str,
        now: Optional[float] = None,
        expected_attempt_count: Optional[int] = None,
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
            if row["status"] in TERMINAL_STATUSES:
                connection.commit()
                return self._task(row)
            if row["status"] != TaskStatus.PLANNING.value:
                connection.rollback()
                raise ConflictError(
                    f"invalid transition: {row['status']} -> {TaskStatus.PENDING.value}"
                )
            if (
                expected_attempt_count is not None
                and row["planner_attempt_count"] != expected_attempt_count
            ):
                connection.rollback()
                raise ConflictError("planner attempt is stale")
            connection.execute(
                """
                UPDATE tasks SET status = ?, plan = ?, execution_prompt = ?,
                    planner_finished_at = ?, planner_lease_expires_at = NULL,
                    error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (
                    TaskStatus.PENDING.value,
                    plan,
                    execution_prompt,
                    current,
                    current,
                    task_id,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            connection.commit()
        return self._task(updated)

    def fail_planning(
        self,
        task_id: str,
        status: str,
        error: str,
        retryable: bool = False,
        now: Optional[float] = None,
        expected_attempt_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        if status not in (TaskStatus.FAILED.value, TaskStatus.TIMED_OUT.value):
            raise ConflictError("planning failure status must be failed or timed_out")
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
            if row["status"] in TERMINAL_STATUSES:
                connection.commit()
                return self._task(row)
            if row["status"] != TaskStatus.PLANNING.value:
                connection.rollback()
                raise ConflictError(f"cannot fail planner from {row['status']}")
            if (
                expected_attempt_count is not None
                and row["planner_attempt_count"] != expected_attempt_count
            ):
                connection.rollback()
                raise ConflictError("planner attempt is stale")
            if retryable and row["planner_attempt_count"] < row["planner_max_attempts"]:
                connection.execute(
                    """
                    UPDATE tasks SET status = ?, planner_lease_expires_at = NULL,
                        error = ?, updated_at = ? WHERE id = ?
                    """,
                    (TaskStatus.PLANNING_PENDING.value, error, current, task_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE tasks SET status = ?, planner_finished_at = ?,
                        planner_lease_expires_at = NULL, finished_at = ?,
                        error = ?, updated_at = ? WHERE id = ?
                    """,
                    (status, current, current, error, current, task_id),
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
                        updated_at = ?, error = ?, resolved_worker_id = NULL,
                        resolved_route_id = NULL, resolved_gateway_id = NULL,
                        resolved_profile = NULL, resolved_execution_agent = NULL
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
            if row["status"] in (
                TaskStatus.PLANNING_PENDING.value,
                TaskStatus.PLANNING.value,
                TaskStatus.PENDING.value,
            ):
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
        planning_rows = connection.execute(
            """
            SELECT * FROM tasks
            WHERE status = ? AND planner_lease_expires_at IS NOT NULL
              AND planner_lease_expires_at < ?
            """,
            (TaskStatus.PLANNING.value, now),
        ).fetchall()
        for row in planning_rows:
            if row["planner_attempt_count"] < row["planner_max_attempts"]:
                connection.execute(
                    """
                    UPDATE tasks SET status = ?, planner_lease_expires_at = NULL,
                        updated_at = ?, error = ? WHERE id = ?
                    """,
                    (
                        TaskStatus.PLANNING_PENDING.value,
                        now,
                        "planner lease expired; task requeued",
                        row["id"],
                    ),
                )
                continue
            connection.execute(
                """
                UPDATE tasks SET status = ?, planner_lease_expires_at = NULL,
                    planner_finished_at = ?, finished_at = ?, updated_at = ?,
                    error = COALESCE(error, ?) WHERE id = ?
                """,
                (
                    TaskStatus.TIMED_OUT.value,
                    now,
                    now,
                    now,
                    "planner lease expired",
                    row["id"],
                ),
            )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (row["id"],)
            ).fetchone()
            self._enqueue_notification(connection, updated, now)

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
                        updated_at = ?, error = ?, resolved_worker_id = NULL,
                        resolved_route_id = NULL, resolved_gateway_id = NULL,
                        resolved_profile = NULL, resolved_execution_agent = NULL WHERE id = ?
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
