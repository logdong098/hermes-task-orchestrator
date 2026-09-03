from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import httpx

from hermes.config import CoordinatorSettings
from hermes.coordinator import create_app
from hermes.security import compact_json, sign_request
from hermes.storage import SQLiteStore


class RouteAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        self.settings = CoordinatorSettings(
            database_path=database,
            director_api_key="director-test-key",
            worker_shared_secret="worker-test-secret",
        )
        self.store = SQLiteStore(database)
        self.store.initialize()
        app = create_app(self.settings, self.store)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.temporary_directory.cleanup()

    async def worker_post(self, path: str, payload=None) -> httpx.Response:
        body = compact_json(payload)
        headers = sign_request("worker-test-secret", "POST", path, body)
        headers["X-Hermes-Worker-ID"] = "gateway-worker"
        if payload is not None:
            headers["Content-Type"] = "application/json"
        return await self.client.post(path, content=body, headers=headers)

    async def command_worker_post(self, path: str, payload=None) -> httpx.Response:
        body = compact_json(payload)
        headers = sign_request("worker-test-secret", "POST", path, body)
        headers["X-Hermes-Worker-ID"] = "command-worker"
        if payload is not None:
            headers["Content-Type"] = "application/json"
        return await self.client.post(path, content=body, headers=headers)

    async def register_gateway_worker(self) -> httpx.Response:
        return await self.worker_post(
            "/api/v1/workers/register",
            {
                "worker_id": "gateway-worker",
                "name": "Gateway Worker",
                "max_concurrency": 1,
                "capabilities": ["agent:hermes"],
                "metadata": {"platform": "test"},
                "worker_kind": "gateway",
                "default_agent": "hermes",
                "routes": [
                    {
                        "route_id": "gateway-worker:homelab:architect",
                        "gateway_id": "homelab",
                        "profile": "architect",
                        "target_profile": "architect",
                        "gateway_kind": "remote",
                        "supported_agents": ["hermes"],
                        "default_agent": "hermes",
                        "labels": {"device": "homelab"},
                    }
                ],
            },
        )

    async def test_task_route_fields_are_paired(self) -> None:
        response = await self.client.post(
            "/api/v1/tasks",
            json={"prompt": "hello", "target_gateway_id": "homelab"},
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(422, response.status_code)

        response = await self.client.post(
            "/api/v1/tasks",
            json={
                "prompt": "hello",
                "target_gateway_id": "homelab",
                "target_profile": "architect",
            },
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual("homelab", response.json()["target_gateway_id"])
        self.assertEqual("architect", response.json()["target_profile"])

    async def test_task_agent_alias_is_persisted_as_canonical_id(self) -> None:
        response = await self.client.post(
            "/api/v1/tasks",
            json={"prompt": "use Claude Code", "execution_agent": "claude"},
            headers={"Authorization": "Bearer director-test-key"},
        )

        self.assertEqual(201, response.status_code, response.text)
        self.assertEqual("claude-code", response.json()["execution_agent"])

    async def test_codex_with_chatgpt_plan_round_trip_unlocks_worker(self) -> None:
        created = await self.client.post(
            "/api/v1/tasks",
            json={
                "prompt": "implement the feature",
                "execution_agent": "cc",
            },
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(201, created.status_code, created.text)
        task_id = created.json()["id"]
        self.assertEqual("planning_pending", created.json()["status"])
        self.assertEqual("codex-with-chatgpt", created.json()["planner_agent"])

        unauthorized = await self.client.post("/api/v1/planner/tasks/claim")
        self.assertEqual(401, unauthorized.status_code)

        claimed = await self.client.post(
            "/api/v1/planner/tasks/claim",
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(200, claimed.status_code, claimed.text)
        planning_task = claimed.json()["task"]
        self.assertEqual(task_id, planning_task["id"])
        self.assertEqual("codex-with-chatgpt", claimed.json()["protocol"]["name"])
        self.assertIn("STATE: INIT", claimed.json()["protocol"]["init"])
        self.assertTrue(planning_task["planner_claim_token"])

        rejected = await self.client.post(
            f"/api/v1/tasks/{task_id}/plan",
            json={
                "planner_claim_token": "x" * 32,
                "plan": "stale plan",
            },
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(409, rejected.status_code, rejected.text)

        planned = await self.client.post(
            f"/api/v1/tasks/{task_id}/plan",
            json={
                "planner_claim_token": planning_task["planner_claim_token"],
                "plan": "Edit the feature and run the regression tests.",
            },
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(200, planned.status_code, planned.text)
        self.assertEqual("pending", planned.json()["status"])
        self.assertEqual(
            "Edit the feature and run the regression tests.", planned.json()["plan"]
        )
        self.assertIn("Original development task", planned.json()["execution_prompt"])

        registered = await self.command_worker_post(
            "/api/v1/workers/register",
            {
                "worker_id": "command-worker",
                "name": "Command Worker",
                "max_concurrency": 1,
                "capabilities": ["agent:claude-code"],
                "metadata": {"default_agent": "claude-code"},
            },
        )
        self.assertEqual(200, registered.status_code, registered.text)
        worker_claim = await self.command_worker_post(
            "/api/v1/workers/command-worker/tasks/claim"
        )
        self.assertEqual(200, worker_claim.status_code, worker_claim.text)
        self.assertEqual(task_id, worker_claim.json()["task"]["id"])
        self.assertEqual(
            "claude-code", worker_claim.json()["task"]["resolved_execution_agent"]
        )

    async def test_routes_require_director_auth_and_are_non_secret(self) -> None:
        response = await self.register_gateway_worker()
        self.assertEqual(200, response.status_code, response.text)

        unauthorized = await self.client.get("/api/v1/routes")
        self.assertEqual(401, unauthorized.status_code)
        response = await self.client.get(
            "/api/v1/routes",
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(200, response.status_code, response.text)
        route = response.json()[0]
        self.assertEqual("homelab", route["gateway_id"])
        self.assertEqual("architect", route["profile"])
        self.assertIsNone(route["availability_reason"])
        serialized = str(route).lower()
        self.assertNotIn("token", serialized)
        self.assertNotIn("secret", serialized)
        self.assertNotIn("authorization", serialized)

    async def test_exact_route_claim_and_remote_run_audit(self) -> None:
        response = await self.register_gateway_worker()
        self.assertEqual(200, response.status_code, response.text)
        task = self.store.create_task(
            {
                "prompt": "implement feature",
                "planner_agent": None,
                "target_gateway_id": "homelab",
                "target_profile": "architect",
            },
            default_timeout_seconds=120,
            max_timeout_seconds=120,
            default_max_attempts=2,
        )

        response = await self.worker_post("/api/v1/workers/gateway-worker/tasks/claim")
        self.assertEqual(200, response.status_code, response.text)
        claimed = response.json()["task"]
        self.assertEqual(task["id"], claimed["id"])
        self.assertEqual("gateway-worker", claimed["resolved_worker_id"])
        self.assertEqual("homelab", claimed["resolved_gateway_id"])
        self.assertEqual("architect", claimed["resolved_profile"])
        self.assertEqual("hermes", claimed["resolved_execution_agent"])

        missing_fence = await self.worker_post(
            f"/api/v1/tasks/{task['id']}/progress",
            {"phase": "stale"},
        )
        self.assertEqual(422, missing_fence.status_code)
        stale_fence = await self.worker_post(
            f"/api/v1/tasks/{task['id']}/progress",
            {"claim_token": "b" * 32, "phase": "stale"},
        )
        self.assertEqual(409, stale_fence.status_code)

        response = await self.worker_post(
            f"/api/v1/tasks/{task['id']}/progress",
            {
                "claim_token": claimed["claim_token"],
                "phase": "starting_remote_run",
                "message": "starting remote run",
                "details": {"attempt": 1},
            },
        )
        self.assertEqual(200, response.status_code, response.text)
        self.assertEqual("starting_remote_run", response.json()["current_phase"])

        response = await self.worker_post(
            f"/api/v1/tasks/{task['id']}/remote-run",
            {
                "claim_token": claimed["claim_token"],
                "remote_run_id": "run-123",
                "remote_session_id": "session-123",
            },
        )
        self.assertEqual(200, response.status_code, response.text)
        self.assertEqual("run-123", response.json()["remote_run_id"])
        self.assertEqual("session-123", response.json()["remote_session_id"])

        events = await self.client.get(
            f"/api/v1/tasks/{task['id']}/events",
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(200, events.status_code, events.text)
        self.assertEqual(
            [
                "task_created",
                "task_claimed",
                "worker_progress",
                "remote_run_attached",
            ],
            [event["event_type"] for event in events.json()],
        )

    async def test_pending_exact_task_reports_unavailable_route(self) -> None:
        task = self.store.create_task(
            {
                "prompt": "wait for missing route",
                "planner_agent": None,
                "target_gateway_id": "missing-gateway",
                "target_profile": "architect",
            },
            default_timeout_seconds=120,
            max_timeout_seconds=120,
            default_max_attempts=2,
        )
        response = await self.client.get(
            f"/api/v1/tasks/{task['id']}",
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            "no registered route matches the requested Gateway/Profile",
            response.json()["routing_diagnostic"],
        )

    async def test_targeted_gateway_worker_preserves_agent_constraint(self) -> None:
        response = await self.register_gateway_worker()
        self.assertEqual(200, response.status_code, response.text)
        task = self.store.create_task(
            {
                "prompt": "run with Claude Code",
                "planner_agent": None,
                "target_worker_id": "gateway-worker",
                "execution_agent": "claude",
            },
            default_timeout_seconds=120,
            max_timeout_seconds=120,
            default_max_attempts=2,
        )

        response = await self.client.get(
            f"/api/v1/tasks/{task['id']}",
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(200, response.status_code, response.text)
        self.assertEqual(
            "matching route does not support the requested execution agent",
            response.json()["routing_diagnostic"],
        )

        response = await self.worker_post("/api/v1/workers/gateway-worker/tasks/claim")
        self.assertEqual(200, response.status_code, response.text)
        self.assertIsNone(response.json()["task"])
        self.assertEqual("pending", self.store.get_task(task["id"])["status"])


if __name__ == "__main__":
    unittest.main()
