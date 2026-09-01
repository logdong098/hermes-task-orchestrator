from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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

    def test_task_state_transition_and_notification(self) -> None:
        task = self.create_task(telegram_chat_id="123")
        claimed = self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.assertEqual(task["id"], claimed["id"])
        self.assertEqual("claimed", claimed["status"])
        running = self.store.update_task_status(
            task["id"], "worker-a", "running", now=102
        )
        self.assertEqual("running", running["status"])
        finished = self.store.report_result(
            task["id"],
            "worker-a",
            "succeeded",
            "done",
            None,
            False,
            now=103,
        )
        self.assertEqual("succeeded", finished["status"])
        notifications = self.store.list_notifications()
        self.assertEqual(1, len(notifications))
        self.assertEqual("done", notifications[0]["payload"]["result"])

    def test_invalid_transition_is_rejected(self) -> None:
        task = self.create_task()
        self.store.claim_task("worker-a", lease_seconds=30, now=101)
        with self.assertRaises(ConflictError):
            self.store.update_task_status(task["id"], "worker-a", "succeeded")
        with self.assertRaises(ConflictError):
            self.store.report_result(
                task["id"],
                "worker-a",
                "succeeded",
                "invalid",
                None,
                False,
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
        self.store.claim_task("worker-a", lease_seconds=10, now=101)
        self.store.update_task_status(task["id"], "worker-a", "running", now=102)
        expired = self.store.report_result(
            task["id"],
            "worker-a",
            "succeeded",
            "too late",
            None,
            False,
            now=112,
        )
        self.assertEqual("timed_out", expired["status"])
        self.assertIsNone(expired["result"])

    def test_idempotency_key_returns_existing_task(self) -> None:
        first = self.create_task(idempotency_key="telegram-update:1")
        second = self.create_task(
            idempotency_key="telegram-update:1", prompt="duplicate prompt"
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual("test task", second["prompt"])

    def test_running_task_cancellation(self) -> None:
        task = self.create_task()
        self.store.claim_task("worker-a", lease_seconds=30, now=101)
        self.store.update_task_status(task["id"], "worker-a", "running", now=102)
        cancellation = self.store.cancel_task(task["id"], now=103)
        self.assertEqual("cancel_requested", cancellation["status"])
        cancel_ids = self.store.heartbeat(
            "worker-a", [task["id"]], lease_seconds=30, now=104
        )
        self.assertEqual([task["id"]], cancel_ids)
        final = self.store.report_result(
            task["id"], "worker-a", "cancelled", None, "stopped", False, now=105
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


if __name__ == "__main__":
    unittest.main()
