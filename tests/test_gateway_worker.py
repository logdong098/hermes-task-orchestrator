import unittest
from unittest.mock import AsyncMock

import httpx

from hermes.config import GatewayWorkerSettings
from hermes.gateway_worker import GatewayWorker


class FakeAPI:
    def __init__(self):
        self.reports = []
        self.running = []
        self.attached = []
        self.registrations = []
        self.set_running_error = None
        self.attach_error = None

    async def register(self, name, max_concurrency, capabilities, metadata, **extra):
        self.registrations.append(
            (name, max_concurrency, capabilities, metadata, extra)
        )
        return {}

    async def claim(self):
        return None

    async def heartbeat(self, running_task_ids):
        return []

    async def set_running(self, task_id):
        self.running.append(task_id)
        if self.set_running_error:
            raise self.set_running_error
        return {}

    async def report(self, task_id, status, result=None, error=None, retryable=False):
        self.reports.append((task_id, status, result, error, retryable))
        return {}

    async def _request(self, method, path, payload=None):
        self.attached.append((method, path, payload))
        if self.attach_error:
            raise self.attach_error
        return {}


class FakeGateway:
    def __init__(self, statuses, start_id="run-1", start_session="sess-1"):
        self.statuses = iter(statuses)
        self.start_id = start_id
        self.start_session = start_session
        self.starts = []
        self.gets = []
        self.stops = []

    async def start_run(self, profile, prompt, key):
        self.starts.append((profile, prompt, key))
        return {
            "id": self.start_id,
            "session_id": self.start_session,
            "status": "queued",
        }

    async def get_run(self, profile, run_id):
        self.gets.append((profile, run_id))
        value = next(self.statuses)
        if isinstance(value, Exception):
            raise value
        return value

    async def stop_run(self, profile, run_id):
        self.stops.append((profile, run_id))
        return {}


def settings():
    return GatewayWorkerSettings(
        worker_id="gw-worker",
        worker_name="Gateway",
        shared_secret="secret",
        gateway_id="gw",
        profiles=["dev"],
        gateway_poll_interval_seconds=0,
        task_timeout_seconds=1,
    )


def task(**extra):
    value = {
        "id": "task-1",
        "prompt": "do it",
        "resolved_profile": "dev",
        "attempt_count": 2,
        "timeout_seconds": 1,
    }
    value.update(extra)
    return value


class GatewayWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_registers_structured_remote_routes(self):
        api = FakeAPI()
        gateway = FakeGateway([])
        remote_settings = GatewayWorkerSettings(
            worker_id="gw-worker",
            worker_name="Gateway",
            shared_secret="secret",
            gateway_id="gw",
            gateway_kind="remote",
            profiles=["dev"],
            poll_interval_seconds=0,
        )
        await GatewayWorker(remote_settings, api, gateway).run(once=True)
        registration = api.registrations[0][4]
        self.assertEqual("gateway", registration["worker_kind"])
        self.assertEqual("remote", registration["routes"][0]["gateway_kind"])
        self.assertEqual("dev", registration["routes"][0]["profile"])
        self.assertIn("agent:hermes", api.registrations[0][2])

    async def test_start_attach_poll_success(self):
        api = FakeAPI()
        gateway = FakeGateway(
            [
                {"status": "running", "session_id": "sess-from-status"},
                {"status": "succeeded", "result": "done"},
            ],
            start_session=None,
        )
        await GatewayWorker(settings(), api, gateway).run_task(task())
        self.assertEqual([("dev", "do it", "task-1-2")], gateway.starts)
        self.assertEqual("run-1", api.attached[0][2]["remote_run_id"])
        self.assertIsNone(api.attached[0][2]["remote_session_id"])
        self.assertEqual("sess-from-status", api.attached[-1][2]["remote_session_id"])
        self.assertEqual("succeeded", api.reports[-1][1])

    async def test_cancel_and_timeout_stop_remote_run(self):
        api = FakeAPI()
        gateway = FakeGateway([{"status": "cancelled"}])
        worker = GatewayWorker(settings(), api, gateway)
        worker.cancelled.add("task-1")
        await worker.run_task(task())
        self.assertEqual("cancelled", api.reports[-1][1])
        self.assertEqual([("dev", "run-1")], gateway.stops)

        api = FakeAPI()
        gateway = FakeGateway([{"status": "cancelled"}])
        await GatewayWorker(settings(), api, gateway).run_task(task(timeout_seconds=0))
        self.assertEqual("timed_out", api.reports[-1][1])
        self.assertEqual([("dev", "run-1")], gateway.stops)

    async def test_reconcile_404_restarts_with_current_attempt(self):
        api = FakeAPI()
        request = httpx.Request("GET", "http://gateway")
        missing = httpx.HTTPStatusError(
            "missing", request=request, response=httpx.Response(404, request=request)
        )
        gateway = FakeGateway([])
        gateway.get_run = AsyncMock(
            side_effect=[missing, {"status": "succeeded", "result": "new"}]
        )
        await GatewayWorker(settings(), api, gateway).run_task(
            task(remote_run_id="old-run")
        )
        self.assertEqual([("dev", "do it", "task-1-2")], gateway.starts)

    async def test_reconcile_error_stops_orphan_before_reporting(self):
        api = FakeAPI()
        request = httpx.Request("GET", "http://gateway")
        failure = httpx.ConnectError("offline", request=request)
        gateway = FakeGateway([])
        gateway.get_run = AsyncMock(side_effect=failure)
        await GatewayWorker(settings(), api, gateway).run_task(
            task(remote_run_id="orphan")
        )
        self.assertEqual([("dev", "orphan")], gateway.stops)
        self.assertEqual("failed", api.reports[-1][1])
        self.assertTrue(api.reports[-1][4])

    async def test_poll_error_stops_started_run_before_reporting(self):
        api = FakeAPI()
        request = httpx.Request("GET", "http://gateway")
        gateway = FakeGateway(
            [
                httpx.ConnectError("offline", request=request),
                {"status": "cancelled"},
            ]
        )

        await GatewayWorker(settings(), api, gateway).run_task(task())

        self.assertEqual([("dev", "run-1")], gateway.stops)
        self.assertEqual("failed", api.reports[-1][1])
        self.assertTrue(api.reports[-1][4])

    async def test_reconcile_terminal_failure_starts_current_attempt(self):
        api = FakeAPI()
        gateway = FakeGateway(
            [{"status": "failed"}, {"status": "succeeded", "result": "new"}]
        )
        await GatewayWorker(settings(), api, gateway).run_task(
            task(remote_run_id="old-run")
        )
        self.assertEqual([("dev", "do it", "task-1-2")], gateway.starts)
        self.assertEqual("succeeded", api.reports[-1][1])

    async def test_set_running_409_never_starts_run(self):
        api = FakeAPI()
        request = httpx.Request("POST", "http://coordinator")
        api.set_running_error = httpx.HTTPStatusError(
            "conflict", request=request, response=httpx.Response(409, request=request)
        )
        gateway = FakeGateway([])
        await GatewayWorker(settings(), api, gateway).run_task(task())
        self.assertFalse(gateway.starts)
        self.assertFalse(api.reports)

    async def test_attach_conflict_stops_unbound_remote_run(self):
        api = FakeAPI()
        request = httpx.Request("POST", "http://coordinator")
        api.attach_error = httpx.HTTPStatusError(
            "conflict", request=request, response=httpx.Response(409, request=request)
        )
        gateway = FakeGateway([{"status": "cancelled"}])
        await GatewayWorker(settings(), api, gateway).run_task(task())
        self.assertEqual([("dev", "run-1")], gateway.stops)
        self.assertEqual("cancelled", api.reports[-1][1])


if __name__ == "__main__":
    unittest.main()
