from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pydantic import ValidationError

from hermes.models import TaskProgressUpdate
from hermes.storage import ConflictError, SQLiteStore


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        self.store = SQLiteStore(database)
        self.store.initialize()
        self.store.register_worker(
            {
                "worker_id": "worker-a",
                "name": "Worker A",
                "max_concurrency": 2,
                "capabilities": ["hermes-chat"],
                "metadata": {},
            },
            now=100,
        )
        self.store.register_worker(
            {
                "worker_id": "worker-b",
                "name": "Worker B",
                "max_concurrency": 2,
                "capabilities": ["hermes-chat"],
                "metadata": {},
            },
            now=100,
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def create_task(self, **overrides):
        payload = {
            "prompt": "test task",
            "timeout_seconds": 60,
            "max_attempts": 2,
            "priority": 0,
        }
        payload.update(overrides)
        return self.store.create_task(payload, 60, 600, 2, now=100)

    def claim_token(self, task_id: str) -> str:
        return self.store.get_task(task_id)["claim_token"]

    def test_task_state_transition_and_notification(self) -> None:
        task = self.create_task(telegram_chat_id="123")
        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.assertEqual(task["id"], claimed["id"])
        self.assertEqual("claimed", claimed["status"])
        running = self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.assertEqual("running", running["status"])
        finished = self.store.report_result(
            task["id"],
            "worker-a",
            "succeeded",
            "done",
            None,
            False,
            claim_token=claimed["claim_token"],
            now=103,
        )
        self.assertEqual("succeeded", finished["status"])
        notifications = self.store.list_notifications()
        self.assertEqual(1, len(notifications))
        self.assertEqual("done", notifications[0]["payload"]["result"])

    def test_invalid_transition_is_rejected(self) -> None:
        task = self.create_task()
        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        with self.assertRaises(ConflictError):
            self.store.update_task_status(
                task["id"],
                "worker-a",
                "succeeded",
                claim_token=claimed["claim_token"],
            )
        with self.assertRaises(ConflictError):
            self.store.report_result(
                task["id"],
                "worker-a",
                "succeeded",
                "invalid",
                None,
                False,
                claim_token=claimed["claim_token"],
                now=102,
            )

    def test_concurrent_claim_delivers_task_once(self) -> None:
        task = self.create_task()
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(
                executor.map(
                    lambda worker_id: self.store.claim_task(
                        worker_id, lease_seconds=30, now=101
                    ),
                    ["worker-a", "worker-b"],
                )
            )
        claimed = [result for result in results if result is not None]
        self.assertEqual(1, len(claimed))
        self.assertEqual(task["id"], claimed[0]["id"])

    def test_heartbeat_staleness(self) -> None:
        workers = self.store.list_workers(stale_seconds=45, now=144)
        self.assertTrue(all(worker["status"] == "online" for worker in workers))
        workers = self.store.list_workers(stale_seconds=45, now=146)
        self.assertTrue(all(worker["status"] == "offline" for worker in workers))
        self.store.heartbeat("worker-a", [], lease_seconds=30, now=147)
        workers = self.store.list_workers(stale_seconds=45, now=148)
        statuses = {worker["worker_id"]: worker["status"] for worker in workers}
        self.assertEqual("online", statuses["worker-a"])
        self.assertEqual("offline", statuses["worker-b"])

    def test_heartbeat_claim_token_fences_stale_runtime_from_new_attempt(self) -> None:
        task = self.create_task(max_attempts=2)
        first = self.store.claim_task("worker-a", lease_seconds=10, now=101)
        self.store.run_maintenance(now=112)
        second = self.store.claim_task("worker-a", lease_seconds=30, now=113)
        self.assertNotEqual(first["claim_token"], second["claim_token"])
        self.assertEqual(143, second["lease_expires_at"])

        self.store.heartbeat(
            "worker-a",
            [],
            lease_seconds=60,
            running_claims=[
                {"task_id": task["id"], "claim_token": first["claim_token"]}
            ],
            now=114,
        )
        after_stale = self.store.get_task(task["id"])
        self.assertEqual(143, after_stale["lease_expires_at"])
        worker = next(
            item
            for item in self.store.list_workers(stale_seconds=1, now=114)
            if item["worker_id"] == "worker-a"
        )
        self.assertEqual(114, worker["last_heartbeat_at"])

        self.store.heartbeat(
            "worker-a",
            [],
            lease_seconds=60,
            running_claims=[
                {"task_id": task["id"], "claim_token": second["claim_token"]}
            ],
            now=115,
        )
        self.assertEqual(175, self.store.get_task(task["id"])["lease_expires_at"])

    def test_legacy_heartbeat_only_renews_tasks_without_claim_tokens(self) -> None:
        task = self.create_task()
        self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.store.heartbeat("worker-a", [task["id"]], lease_seconds=60, now=102)
        self.assertEqual(131, self.store.get_task(task["id"])["lease_expires_at"])

        with self.store._connect() as connection:
            connection.execute(
                "UPDATE tasks SET claim_token = NULL WHERE id = ?", (task["id"],)
            )
        self.store.heartbeat("worker-a", [task["id"]], lease_seconds=60, now=103)
        legacy = self.store.get_task(task["id"])
        self.assertIsNone(legacy["claim_token"])
        self.assertEqual(163, legacy["lease_expires_at"])

    def test_expired_lease_retries_then_times_out(self) -> None:
        task = self.create_task(max_attempts=2)
        self.store.claim_task("worker-a", lease_seconds=10, now=101)
        self.store.run_maintenance(now=112)
        self.assertEqual("pending", self.store.get_task(task["id"])["status"])
        self.store.claim_task("worker-b", lease_seconds=10, now=113)
        self.store.run_maintenance(now=124)
        self.assertEqual("timed_out", self.store.get_task(task["id"])["status"])

    def test_expired_worker_result_cannot_overwrite_task(self) -> None:
        task = self.create_task(max_attempts=1)
        claimed = self.store.claim_task("worker-a", lease_seconds=10, now=101)
        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        with self.assertRaises(ConflictError):
            self.store.report_result(
                task["id"],
                "worker-a",
                "succeeded",
                "too late",
                None,
                False,
                claim_token=claimed["claim_token"],
                now=112,
            )
        expired = self.store.get_task(task["id"])
        self.assertEqual("timed_out", expired["status"])
        self.assertIsNone(expired["result"])

    def test_stale_claim_from_same_worker_cannot_mutate_new_attempt(self) -> None:
        task = self.create_task(max_attempts=2)
        first = self.store.claim_task("worker-a", lease_seconds=10, now=101)
        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=first["claim_token"],
            now=102,
        )
        self.store.run_maintenance(now=112)
        second = self.store.claim_task("worker-a", lease_seconds=30, now=113)
        self.assertNotEqual(first["claim_token"], second["claim_token"])

        with self.assertRaises(ConflictError):
            self.store.update_task_status(
                task["id"],
                "worker-a",
                "running",
                claim_token=first["claim_token"],
                now=114,
            )
        with self.assertRaises(ConflictError):
            self.store.record_progress(
                task["id"],
                "worker-a",
                "stale",
                claim_token=first["claim_token"],
                now=114,
            )

        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=second["claim_token"],
            now=114,
        )
        with self.assertRaises(ConflictError):
            self.store.report_result(
                task["id"],
                "worker-a",
                "succeeded",
                "stale",
                None,
                False,
                claim_token=first["claim_token"],
                now=115,
            )
        final = self.store.report_result(
            task["id"],
            "worker-a",
            "succeeded",
            "current",
            None,
            False,
            claim_token=second["claim_token"],
            now=115,
        )
        self.assertEqual("current", final["result"])

    def test_idempotency_key_returns_existing_task(self) -> None:
        first = self.create_task(idempotency_key="telegram-update:1")
        second = self.create_task(
            idempotency_key="telegram-update:1", prompt="duplicate prompt"
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual("test task", second["prompt"])

    def test_explicit_planning_modes_control_initial_state(self) -> None:
        direct = self.create_task(planning_mode="direct", planner_agent=None)
        planned = self.create_task(planning_mode="plan", planner_agent="codex")
        self.assertEqual("pending", direct["status"])
        self.assertEqual("direct", direct["planning_mode"])
        self.assertEqual("planning_pending", planned["status"])
        self.assertEqual("plan", planned["planning_mode"])

    def test_progress_is_recorded_as_append_only_events(self) -> None:
        task = self.create_task(planning_mode="direct")
        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        updated = self.store.record_progress(
            task["id"],
            "worker-a",
            "process_started",
            "local process started",
            {"pid": 123},
            claim_token=claimed["claim_token"],
            now=103,
        )
        self.assertEqual("process_started", updated["current_phase"])
        events = self.store.list_task_events(task["id"])
        self.assertEqual(
            [
                "task_created",
                "task_claimed",
                "task_running",
                "worker_progress",
            ],
            [event["event_type"] for event in events],
        )
        self.assertEqual({"pid": 123}, events[-1]["details"])

    def test_progress_details_are_bounded(self) -> None:
        with self.assertRaises(ValidationError):
            TaskProgressUpdate(
                claim_token="a" * 32,
                phase="running",
                details={"data": "x" * 9000},
            )
        with self.assertRaises(ValidationError):
            TaskProgressUpdate(
                phase="running",
                claim_token="a" * 32,
                details={"a": {"b": {"c": {"d": {"e": {"f": {"g": 1}}}}}}},
            )

    def test_retry_clears_old_remote_binding_before_next_attempt(self) -> None:
        task = self.create_task(planning_mode="direct", max_attempts=2)
        first_claim = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=first_claim["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "worker-a",
            "run-1",
            claim_token=first_claim["claim_token"],
            now=103,
        )

        requeued = self.store.report_result(
            task["id"],
            "worker-a",
            "failed",
            None,
            "retry",
            True,
            remote_run_id="run-1",
            claim_token=first_claim["claim_token"],
            now=104,
        )
        self.assertEqual("pending", requeued["status"])
        self.assertIsNone(requeued["remote_run_id"])
        self.assertIsNone(requeued["remote_run_attempt"])

        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=105)
        self.assertEqual(2, claimed["attempt_count"])
        rebound = self.store.attach_remote_run(
            task["id"],
            "worker-a",
            "run-2",
            claim_token=claimed["claim_token"],
            now=106,
        )
        self.assertEqual("run-2", rebound["remote_run_id"])
        self.assertEqual(2, rebound["remote_run_attempt"])

    def test_initialize_backfills_legacy_remote_run_attempt(self) -> None:
        task = self.create_task(planning_mode="direct")
        with sqlite3.connect(self.store.database_path) as connection:
            connection.execute(
                """
                UPDATE tasks SET remote_run_id = ?, remote_run_attempt = NULL,
                    attempt_count = ? WHERE id = ?
                """,
                ("legacy-run", 3, task["id"]),
            )

        self.store.initialize()

        migrated = self.store.get_task(task["id"])
        self.assertEqual(3, migrated["remote_run_attempt"])

    def test_worker_cannot_claim_deadline_exceeded_early(self) -> None:
        task = self.create_task(planning_mode="direct", timeout_seconds=60)
        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "worker-a",
            "run-1",
            claim_token=claimed["claim_token"],
            now=103,
        )

        with self.assertRaises(ConflictError):
            self.store.mark_reconciling(
                task["id"],
                "worker-a",
                "pretend timeout",
                deadline_exceeded=True,
                claim_token=claimed["claim_token"],
                now=104,
            )

    def test_running_task_cancellation(self) -> None:
        task = self.create_task()
        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.store.update_task_status(
            task["id"],
            "worker-a",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        cancellation = self.store.cancel_task(task["id"], now=103)
        self.assertEqual("cancel_requested", cancellation["status"])
        cancel_ids = self.store.heartbeat(
            "worker-a", [task["id"]], lease_seconds=30, now=104
        )
        self.assertEqual([task["id"]], cancel_ids)
        final = self.store.report_result(
            task["id"],
            "worker-a",
            "cancelled",
            None,
            "stopped",
            False,
            claim_token=claimed["claim_token"],
            now=105,
        )
        self.assertEqual("cancelled", final["status"])

    def test_planning_stage_persists_artifacts_before_worker_claim(self) -> None:
        task = self.create_task(planner_agent="codex", execution_agent="codex")
        self.assertEqual("planning_pending", task["status"])
        self.assertIsNone(self.store.claim_task("worker-a", 30, now=101))

        planning = self.store.claim_planning_task(30, now=101)
        self.assertEqual(task["id"], planning["id"])
        self.assertEqual("planning", planning["status"])
        planned = self.store.complete_planning(
            task["id"], "1. add tests", "original plus plan", now=102
        )
        self.assertEqual("pending", planned["status"])
        self.assertEqual("1. add tests", planned["plan"])
        self.assertEqual("original plus plan", planned["execution_prompt"])

    def test_explicit_agent_is_only_claimed_by_compatible_worker(self) -> None:
        self.store.register_worker(
            {
                "worker_id": "worker-a",
                "name": "Worker A",
                "max_concurrency": 2,
                "capabilities": ["agent:codex"],
                "metadata": {"default_agent": "codex"},
            },
            now=100,
        )
        self.store.register_worker(
            {
                "worker_id": "worker-b",
                "name": "Worker B",
                "max_concurrency": 2,
                "capabilities": ["agent:claude"],
                "metadata": {"default_agent": "claude"},
            },
            now=100,
        )
        claude_task = self.create_task(execution_agent="claude", priority=10)
        codex_task = self.create_task(execution_agent="codex", priority=1)
        self.assertEqual(
            codex_task["id"], self.store.claim_task("worker-a", 30, now=101)["id"]
        )
        self.assertEqual(
            claude_task["id"], self.store.claim_task("worker-b", 30, now=101)["id"]
        )

    def test_expired_planner_lease_retries_then_times_out(self) -> None:
        task = self.store.create_task(
            {"prompt": "plan me", "planner_agent": "codex"},
            60,
            600,
            1,
            default_planner_max_attempts=2,
            now=100,
        )
        self.store.claim_planning_task(10, now=101)
        self.store.run_maintenance(now=112)
        self.assertEqual("planning_pending", self.store.get_task(task["id"])["status"])
        self.store.claim_planning_task(10, now=113)
        self.store.run_maintenance(now=124)
        self.assertEqual("timed_out", self.store.get_task(task["id"])["status"])

    def test_stale_planner_attempt_cannot_complete_new_attempt(self) -> None:
        task = self.store.create_task(
            {"prompt": "plan me", "planner_agent": "codex"},
            60,
            600,
            1,
            default_planner_max_attempts=2,
            now=100,
        )
        first = self.store.claim_planning_task(10, now=101)
        self.store.run_maintenance(now=112)
        second = self.store.claim_planning_task(10, now=113)
        self.assertEqual(1, first["planner_attempt_count"])
        self.assertEqual(2, second["planner_attempt_count"])
        with self.assertRaises(ConflictError):
            self.store.complete_planning(
                task["id"],
                "stale plan",
                "stale execution prompt",
                now=114,
                expected_attempt_count=first["planner_attempt_count"],
            )
        current = self.store.get_task(task["id"])
        self.assertEqual("planning", current["status"])
        self.assertIsNone(current["plan"])

    def test_legacy_database_migration_preserves_pending_task(self) -> None:
        database = str(Path(self.temporary_directory.name) / "legacy.db")
        with sqlite3.connect(database) as connection:
            connection.execute(
                """
                CREATE TABLE tasks (
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
                    lease_expires_at REAL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO tasks (
                    id, prompt, status, timeout_seconds, max_attempts,
                    created_at, updated_at
                ) VALUES ('legacy-task', 'legacy prompt', 'pending', 60, 2, 1, 1)
                """
            )

        store = SQLiteStore(database)
        store.initialize()
        migrated = store.get_task("legacy-task")
        self.assertEqual("pending", migrated["status"])
        self.assertIsNone(migrated["planner_agent"])
        self.assertIsNone(migrated["execution_prompt"])
        self.assertEqual(0, migrated["planner_attempt_count"])
        self.assertEqual(2, migrated["planner_max_attempts"])

        store.register_worker(
            {
                "worker_id": "legacy-worker",
                "name": "Legacy Worker",
                "max_concurrency": 1,
                "capabilities": ["hermes-chat"],
                "metadata": {},
            },
            now=2,
        )
        claimed = store.claim_task("legacy-worker", lease_seconds=30, now=3)
        self.assertEqual("legacy-task", claimed["id"])
        self.assertEqual("legacy prompt", claimed["prompt"])

    def test_legacy_planning_and_gateway_rows_migrate_idempotently(self) -> None:
        database = str(Path(self.temporary_directory.name) / "legacy-active.db")
        with sqlite3.connect(database) as connection:
            connection.execute(
                """
                CREATE TABLE tasks (
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
                    target_gateway_id TEXT,
                    target_profile TEXT,
                    resolved_worker_id TEXT,
                    resolved_route_id TEXT,
                    resolved_gateway_id TEXT,
                    resolved_profile TEXT,
                    resolved_execution_agent TEXT,
                    remote_run_id TEXT,
                    remote_session_id TEXT,
                    result TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    lease_expires_at REAL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO tasks (
                    id, prompt, status, timeout_seconds, max_attempts,
                    planner_agent, created_at, updated_at
                ) VALUES ('legacy-plan', 'plan', 'planning_pending', 60, 2,
                    'codex', 1, 1)
                """
            )
            connection.execute(
                """
                INSERT INTO tasks (
                    id, prompt, status, timeout_seconds, max_attempts,
                    created_at, updated_at
                ) VALUES ('legacy-direct', 'direct', 'pending', 60, 2, 1, 1)
                """
            )
            connection.execute(
                """
                INSERT INTO tasks (
                    id, prompt, status, claimed_by, timeout_seconds,
                    max_attempts, attempt_count, target_gateway_id,
                    target_profile, resolved_worker_id, resolved_route_id,
                    resolved_gateway_id, resolved_profile,
                    resolved_execution_agent, remote_run_id,
                    remote_session_id, created_at, updated_at, started_at,
                    lease_expires_at
                ) VALUES (
                    'legacy-gateway', 'resume', 'running', 'gateway-worker',
                    600, 3, 2, 'homelab', 'architect', 'gateway-worker',
                    'homelab:architect', 'homelab', 'architect', 'hermes',
                    'legacy-run', 'legacy-session', 1, 1, 1, 10
                )
                """
            )

        store = SQLiteStore(database)
        store.initialize()
        store.initialize()

        self.assertEqual("plan", store.get_task("legacy-plan")["planning_mode"])
        self.assertEqual("direct", store.get_task("legacy-direct")["planning_mode"])
        gateway_task = store.get_task("legacy-gateway")
        self.assertEqual(2, gateway_task["remote_run_attempt"])

        store.register_worker(
            {
                "worker_id": "gateway-worker",
                "name": "Gateway Worker",
                "max_concurrency": 1,
                "capabilities": ["agent:hermes"],
                "metadata": {},
                "worker_kind": "gateway",
                "default_agent": "hermes",
                "routes": [
                    {
                        "route_id": "homelab:architect",
                        "gateway_id": "homelab",
                        "profile": "architect",
                        "target_profile": "architect",
                        "gateway_kind": "remote",
                        "supported_agents": ["hermes"],
                        "default_agent": "hermes",
                        "labels": {},
                    }
                ],
            },
            now=11,
        )
        store.run_maintenance(now=12)
        reconciling = store.get_task("legacy-gateway")
        self.assertEqual("reconciling", reconciling["status"])
        reclaimed = store.claim_task("gateway-worker", 30, now=12)
        self.assertEqual("legacy-gateway", reclaimed["id"])
        self.assertEqual("legacy-run", reclaimed["remote_run_id"])
        self.assertEqual(2, reclaimed["attempt_count"])
        self.assertEqual(32, len(reclaimed["claim_token"]))


if __name__ == "__main__":
    unittest.main()
