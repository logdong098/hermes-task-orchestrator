from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
