from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from hermes.config import CoordinatorSettings
from hermes.coordinator import create_app
from hermes.storage import SQLiteStore


class CoordinatorMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_maintenance_expires_abandoned_task(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = str(Path(temporary_directory) / "hermes.db")
            settings = CoordinatorSettings(
                database_path=database,
                director_api_key="director-test-key",
                worker_shared_secret="worker-test-secret",
                maintenance_interval_seconds=0.02,
            )
            store = SQLiteStore(database)
            app = create_app(settings, store)
            async with app.router.lifespan_context(app):
                current = time.time()
                store.register_worker(
                    {
                        "worker_id": "worker-a",
                        "name": "Worker A",
                        "max_concurrency": 1,
                        "capabilities": ["hermes-chat"],
                        "metadata": {},
                    },
                    now=current,
                )
                task = store.create_task(
                    {"prompt": "expire", "max_attempts": 1},
                    60,
                    600,
                    1,
                    now=current,
                )
                store.claim_task("worker-a", lease_seconds=0.02, now=current)
                await asyncio.sleep(0.15)
                self.assertEqual("timed_out", store.get_task(task["id"])["status"])


if __name__ == "__main__":
    unittest.main()
