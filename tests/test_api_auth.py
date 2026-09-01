from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import httpx

from hermes.config import CoordinatorSettings
from hermes.coordinator import create_app
from hermes.security import compact_json, sign_request
from hermes.storage import SQLiteStore


class CoordinatorAuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        settings = CoordinatorSettings(
            database_path=database,
            director_api_key="director-test-key",
            worker_shared_secret="worker-test-secret",
        )
        self.store = SQLiteStore(database)
        self.store.initialize()
        app = create_app(settings, self.store)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.temporary_directory.cleanup()

    async def test_director_bearer_authentication(self) -> None:
        response = await self.client.post("/api/v1/tasks", json={"prompt": "hello"})
        self.assertEqual(401, response.status_code)
        response = await self.client.post(
            "/api/v1/tasks",
            json={"prompt": "hello"},
            headers={"Authorization": "Bearer director-test-key"},
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual("planning_pending", response.json()["status"])

    async def test_worker_hmac_authentication(self) -> None:
        path = "/api/v1/workers/register"
        payload = {
            "worker_id": "worker-a",
            "name": "Worker A",
            "max_concurrency": 1,
            "capabilities": ["hermes-chat"],
            "metadata": {},
        }
        body = compact_json(payload)
        response = await self.client.post(
            path,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Hermes-Worker-ID": "worker-a",
                "X-Hermes-Timestamp": "1",
                "X-Hermes-Signature": "bad",
            },
        )
        self.assertEqual(401, response.status_code)

        headers = sign_request("worker-test-secret", "POST", path, body)
        headers.update(
            {
                "Content-Type": "application/json",
                "X-Hermes-Worker-ID": "worker-a",
            }
        )
        response = await self.client.post(path, content=body, headers=headers)
        self.assertEqual(200, response.status_code)
        self.assertEqual("worker-a", response.json()["worker_id"])
        replay = await self.client.post(path, content=body, headers=headers)
        self.assertEqual(409, replay.status_code)

    async def test_per_worker_secret_rejects_unknown_worker(self) -> None:
        database = str(Path(self.temporary_directory.name) / "isolated.db")
        settings = CoordinatorSettings(
            database_path=database,
            director_api_key="director-test-key",
            worker_secrets={"worker-a": "secret-a"},
        )
        store = SQLiteStore(database)
        store.initialize()
        app = create_app(settings, store)
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://isolated"
        )
        try:
            path = "/api/v1/workers/register"
            payload = {
                "worker_id": "worker-b",
                "name": "Worker B",
                "max_concurrency": 1,
                "capabilities": ["hermes-chat"],
                "metadata": {},
            }
            body = compact_json(payload)
            headers = sign_request("secret-a", "POST", path, body)
            headers.update(
                {
                    "Content-Type": "application/json",
                    "X-Hermes-Worker-ID": "worker-b",
                }
            )
            response = await client.post(path, content=body, headers=headers)
            self.assertEqual(503, response.status_code)
        finally:
            await client.aclose()


if __name__ == "__main__":
    unittest.main()
