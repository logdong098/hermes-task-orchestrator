from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from hermes.storage import ConflictError, SQLiteStore


class StorageRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        self.store = SQLiteStore(database)
        self.store.initialize()
        self.store.register_worker(
            {
                "worker_id": "command-worker",
                "name": "Command Worker",
                "max_concurrency": 2,
                "capabilities": ["agent:codex"],
                "metadata": {},
                "worker_kind": "command",
                "default_agent": "codex",
            },
            now=100,
        )
        self.store.register_worker(
            {
                "worker_id": "gateway-worker",
                "name": "Gateway Worker",
                "max_concurrency": 4,
                "capabilities": ["agent:hermes", "agent:claude"],
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
                    },
                    {
                        "route_id": "homelab:coder",
                        "gateway_id": "homelab",
                        "profile": "coder",
                        "target_profile": "coder",
                        "gateway_kind": "remote",
                        "supported_agents": ["claude"],
                        "default_agent": "claude",
                        "labels": {},
                    },
                ],
            },
            now=100,
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def create_task(self, **overrides):
        payload = {
            "prompt": "route task",
            "timeout_seconds": 60,
            "max_attempts": 2,
            "priority": 0,
        }
        payload.update(overrides)
        return self.store.create_task(payload, 60, 600, 2, now=100)

    def test_exact_route_never_falls_back_to_another_profile(self) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="missing",
        )
        self.assertIsNone(self.store.claim_task("gateway-worker", 30, now=101))
        self.assertIsNone(self.store.claim_task("command-worker", 30, now=101))
        self.assertEqual("pending", self.store.get_task(task["id"])["status"])

    def test_generic_task_is_only_claimed_by_command_worker(self) -> None:
        task = self.create_task()
        self.assertIsNone(self.store.claim_task("gateway-worker", 30, now=101))
        claimed = self.store.claim_task("command-worker", 30, now=101)
        self.assertEqual(task["id"], claimed["id"])
        self.assertEqual("codex", claimed["resolved_execution_agent"])
        self.assertIsNone(claimed["resolved_gateway_id"])

    def test_same_gateway_profiles_keep_distinct_agents(self) -> None:
        architect = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
            priority=10,
        )
        coder = self.create_task(
            target_gateway_id="homelab",
            target_profile="coder",
            priority=5,
        )
        first = self.store.claim_task("gateway-worker", 30, now=101)
        second = self.store.claim_task("gateway-worker", 30, now=101)
        self.assertEqual(architect["id"], first["id"])
        self.assertEqual("architect", first["resolved_profile"])
        self.assertEqual("hermes", first["resolved_execution_agent"])
        self.assertEqual(coder["id"], second["id"])
        self.assertEqual("coder", second["resolved_profile"])
        self.assertEqual("claude", second["resolved_execution_agent"])

    def test_stable_route_id_moves_to_replacement_worker(self) -> None:
        replacement = self.store.register_worker(
            {
                "worker_id": "replacement-gateway",
                "name": "Replacement Gateway",
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
                        "labels": {"generation": "replacement"},
                    }
                ],
            },
            now=101,
        )

        self.assertEqual("replacement-gateway", replacement["worker_id"])
        route = next(
            item
            for item in self.store.list_routes(now=101)
            if item["route_id"] == "homelab:architect"
        )
        self.assertEqual("replacement-gateway", route["worker_id"])
        self.assertEqual({"generation": "replacement"}, route["labels"])
        workers = {
            item["worker_id"]: item for item in self.store.list_workers(45, now=101)
        }
        self.assertEqual(
            ["homelab:coder"],
            [route["route_id"] for route in workers["gateway-worker"]["routes"]],
        )

    def test_target_profile_alias_resolves_to_backend_profile(self) -> None:
        self.store.register_worker(
            {
                "worker_id": "alias-worker",
                "name": "Alias Worker",
                "max_concurrency": 1,
                "capabilities": ["agent:hermes"],
                "metadata": {},
                "worker_kind": "gateway",
                "default_agent": "hermes",
                "routes": [
                    {
                        "route_id": "alias-route",
                        "gateway_id": "alias-gateway",
                        "profile": "backend-profile",
                        "target_profile": "public-alias",
                        "gateway_kind": "remote",
                        "supported_agents": ["hermes"],
                        "default_agent": "hermes",
                        "labels": {},
                    }
                ],
            },
            now=100,
        )
        task = self.create_task(
            target_gateway_id="alias-gateway",
            target_profile="public-alias",
        )
        claimed = self.store.claim_task("alias-worker", 30, now=101)
        self.assertEqual(task["id"], claimed["id"])
        self.assertEqual("public-alias", claimed["target_profile"])
        self.assertEqual("backend-profile", claimed["resolved_profile"])

    def test_exact_route_agent_does_not_use_worker_union_capability(self) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
            execution_agent="claude",
        )
        self.assertIsNone(self.store.claim_task("gateway-worker", 30, now=101))
        self.assertEqual("pending", self.store.get_task(task["id"])["status"])

    def test_retry_keeps_request_and_event_audit_but_clears_active_binding(
        self,
    ) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        claimed = self.store.claim_task("gateway-worker", 30, now=101)
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            "session-1",
            claim_token=claimed["claim_token"],
            now=103,
        )
        retried = self.store.report_result(
            task["id"],
            "gateway-worker",
            "failed",
            None,
            "temporary failure",
            True,
            remote_run_id="run-1",
            claim_token=claimed["claim_token"],
            now=104,
        )
        self.assertEqual("pending", retried["status"])
        self.assertEqual("homelab", retried["target_gateway_id"])
        self.assertEqual("architect", retried["target_profile"])
        self.assertIsNone(retried["resolved_worker_id"])
        self.assertIsNone(retried["resolved_profile"])
        self.assertIsNone(retried["resolved_execution_agent"])
        self.assertIsNone(retried["remote_run_id"])
        self.assertIsNone(retried["remote_session_id"])
        self.assertIsNone(retried["remote_run_attempt"])
        events = self.store.list_task_events(task["id"])
        attached = next(
            event for event in events if event["event_type"] == "remote_run_attached"
        )
        self.assertEqual("run-1", attached["details"]["remote_run_id"])
        self.assertEqual(1, claimed["attempt_count"])

    def test_expired_lease_cannot_attach_remote_run(self) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        claimed = self.store.claim_task("gateway-worker", 10, now=101)
        with self.assertRaises(ConflictError):
            self.store.attach_remote_run(
                task["id"],
                "gateway-worker",
                "late-run",
                claim_token=claimed["claim_token"],
                now=112,
            )
        current = self.store.get_task(task["id"])
        self.assertEqual("pending", current["status"])
        self.assertIsNone(current["remote_run_id"])

    def test_gateway_lease_expiry_reconciles_same_remote_run(self) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        claimed = self.store.claim_task("gateway-worker", 10, now=101)
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            "session-1",
            claim_token=claimed["claim_token"],
            now=103,
        )

        self.store.run_maintenance(now=112)
        reconciling = self.store.get_task(task["id"])
        self.assertEqual("reconciling", reconciling["status"])
        self.assertEqual("run-1", reconciling["remote_run_id"])
        self.assertEqual("homelab:architect", reconciling["resolved_route_id"])

        reclaimed = self.store.claim_task("gateway-worker", 10, now=112)
        self.assertEqual(task["id"], reclaimed["id"])
        self.assertEqual(claimed["attempt_count"], reclaimed["attempt_count"])
        self.assertNotEqual(claimed["claim_token"], reclaimed["claim_token"])
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=reclaimed["claim_token"],
            now=113,
        )
        succeeded = self.store.report_result(
            task["id"],
            "gateway-worker",
            "succeeded",
            "done",
            None,
            False,
            remote_run_id="run-1",
            claim_token=reclaimed["claim_token"],
            now=114,
        )
        self.assertEqual("succeeded", succeeded["status"])
        event_types = [
            event["event_type"] for event in self.store.list_task_events(task["id"])
        ]
        self.assertIn("reconciliation_started", event_types)
        self.assertIn("reconciliation_claimed", event_types)

    def test_gateway_result_must_match_current_remote_run(self) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        claimed = self.store.claim_task("gateway-worker", 30, now=101)
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            claim_token=claimed["claim_token"],
            now=103,
        )
        with self.assertRaises(ConflictError):
            self.store.report_result(
                task["id"],
                "gateway-worker",
                "succeeded",
                "stale",
                None,
                False,
                remote_run_id="old-run",
                claim_token=claimed["claim_token"],
                now=104,
            )

    def test_reconciliation_deadline_terminalizes_unknown_outcome(self) -> None:
        self.store.reconciliation_grace_seconds = 10
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        claimed = self.store.claim_task("gateway-worker", 10, now=101)
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            claim_token=claimed["claim_token"],
            now=103,
        )
        self.store.run_maintenance(now=112)
        self.store.run_maintenance(now=123)
        current = self.store.get_task(task["id"])
        self.assertEqual("timed_out", current["status"])
        self.assertIn("outcome remained unknown", current["error"])

    def test_unconfirmed_cancellation_expires_as_unknown_not_cancelled(self) -> None:
        self.store.reconciliation_grace_seconds = 10
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        claimed = self.store.claim_task("gateway-worker", 10, now=101)
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=claimed["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            claim_token=claimed["claim_token"],
            now=103,
        )
        self.store.cancel_task(task["id"], now=104)
        self.store.run_maintenance(now=112)
        self.store.run_maintenance(now=123)

        current = self.store.get_task(task["id"])
        self.assertEqual("timed_out", current["status"])
        self.assertIn("cancellation requested", current["error"])
        self.assertIn("outcome remained unknown", current["error"])

    def test_reconciliation_claim_token_rotates_and_fences_stale_gateway(self) -> None:
        task = self.create_task(
            target_gateway_id="homelab",
            target_profile="architect",
        )
        first = self.store.claim_task("gateway-worker", 10, now=101)
        self.store.update_task_status(
            task["id"],
            "gateway-worker",
            "running",
            claim_token=first["claim_token"],
            now=102,
        )
        self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            claim_token=first["claim_token"],
            now=103,
        )
        self.store.run_maintenance(now=112)
        second = self.store.claim_task("gateway-worker", 10, now=112)
        self.assertNotEqual(first["claim_token"], second["claim_token"])

        self.store.heartbeat(
            "gateway-worker",
            [],
            lease_seconds=30,
            running_claims=[
                {"task_id": task["id"], "claim_token": first["claim_token"]}
            ],
            now=113,
        )
        self.assertEqual(122, self.store.get_task(task["id"])["lease_expires_at"])
        self.store.heartbeat(
            "gateway-worker",
            [],
            lease_seconds=30,
            running_claims=[
                {"task_id": task["id"], "claim_token": second["claim_token"]}
            ],
            now=114,
        )
        self.assertEqual(144, self.store.get_task(task["id"])["lease_expires_at"])

        with self.assertRaises(ConflictError):
            self.store.attach_remote_run(
                task["id"],
                "gateway-worker",
                "run-1",
                "stale-session",
                claim_token=first["claim_token"],
                now=113,
            )
        with self.assertRaises(ConflictError):
            self.store.mark_reconciling(
                task["id"],
                "gateway-worker",
                "stale reconciliation",
                claim_token=first["claim_token"],
                now=113,
            )
        current = self.store.attach_remote_run(
            task["id"],
            "gateway-worker",
            "run-1",
            "current-session",
            claim_token=second["claim_token"],
            now=113,
        )
        self.assertEqual("current-session", current["remote_session_id"])

    def test_legacy_command_worker_is_backfilled_as_route(self) -> None:
        database = str(Path(self.temporary_directory.name) / "legacy-worker.db")
        with sqlite3.connect(database) as connection:
            connection.execute(
                """
                CREATE TABLE workers (
                    worker_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    max_concurrency INTEGER NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    registered_at REAL NOT NULL,
                    last_heartbeat_at REAL NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO workers VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "legacy-worker",
                    "Legacy Worker",
                    1,
                    json.dumps(["agent:codex"]),
                    json.dumps({"default_agent": "codex"}),
                    10,
                    20,
                ),
            )
        migrated = SQLiteStore(database)
        migrated.initialize()
        route = migrated.list_routes(stale_seconds=100, now=21)[0]
        self.assertEqual("legacy-worker:command", route["route_id"])
        self.assertEqual(["codex"], route["supported_agents"])
        self.assertEqual("codex", route["default_agent"])


if __name__ == "__main__":
    unittest.main()
