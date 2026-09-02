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
        self.reconciliations = []
        self.progress_updates = []

    async def register(self, name, max_concurrency, capabilities, metadata, **extra):
        self.registrations.append(
            (name, max_concurrency, capabilities, metadata, extra)
        )
        return {}

    async def claim(self):
        return None

    async def heartbeat(self, running_claims):
        return []

    async def set_running(self, task_id, claim_token):
        self.running.append(task_id)
        if self.set_running_error:
            raise self.set_running_error
        return {}

    async def report(
        self,
        task_id,
        claim_token,
        status,
        result=None,
        error=None,
        retryable=False,
        remote_run_id=None,
    ):
        self.reports.append((task_id, status, result, error, retryable, remote_run_id))
        return {}

    async def reconcile(self, task_id, claim_token, reason, deadline_exceeded=False):
        self.reconciliations.append((task_id, reason, deadline_exceeded))
        return {}

    async def progress(self, task_id, claim_token, phase, message=None):
        self.progress_updates.append((task_id, phase, message))
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
        "claim_token": "a" * 32,
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
        self.assertEqual("run-1", api.reports[-1][5])

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
            task(remote_run_id="old-run", remote_run_attempt=2)
        )
        self.assertEqual([("dev", "do it", "task-1-2")], gateway.starts)

    async def test_reconcile_transport_error_preserves_remote_run(self):
        api = FakeAPI()
        request = httpx.Request("GET", "http://gateway")
        failure = httpx.ConnectError("offline", request=request)
        gateway = FakeGateway([])
        gateway.get_run = AsyncMock(side_effect=failure)
        await GatewayWorker(settings(), api, gateway).run_task(
            task(remote_run_id="orphan", remote_run_attempt=2)
        )
        self.assertEqual([], gateway.stops)
        self.assertEqual([], api.reports)
        self.assertEqual("task-1", api.reconciliations[-1][0])
        self.assertIn("reconciliation failed", api.reconciliations[-1][1])

    async def test_old_attempt_binding_is_ignored_and_new_run_starts(self):
        api = FakeAPI()
        gateway = FakeGateway([{"status": "succeeded", "result": "new"}])

        await GatewayWorker(settings(), api, gateway).run_task(
            task(remote_run_id="old-run", remote_run_attempt=1)
        )

        self.assertEqual([("dev", "do it", "task-1-2")], gateway.starts)
        self.assertNotIn(("dev", "old-run"), gateway.gets)
        self.assertEqual("run-1", api.reports[-1][5])

    async def test_poll_error_enters_reconciliation_without_stopping_run(self):
        api = FakeAPI()
        request = httpx.Request("GET", "http://gateway")
        gateway = FakeGateway(
            [
                httpx.ConnectError("offline", request=request),
                {"status": "cancelled"},
            ]
        )

        await GatewayWorker(settings(), api, gateway).run_task(task())

        self.assertEqual([], gateway.stops)
        self.assertEqual([], api.reports)
        self.assertEqual("task-1", api.reconciliations[-1][0])
        self.assertIn("status polling failed", api.reconciliations[-1][1])

    async def test_session_audit_error_enters_reconciliation_without_stopping(self):
        api = FakeAPI()
        request = httpx.Request("POST", "http://coordinator")
        api._request = AsyncMock(
            side_effect=[{}, httpx.ConnectError("offline", request=request)]
        )
        gateway = FakeGateway(
            [{"status": "running", "session_id": "sess-from-status"}],
            start_session=None,
        )

        await GatewayWorker(settings(), api, gateway).run_task(task())

        self.assertEqual([], gateway.stops)
        self.assertEqual([], api.reports)
        self.assertIn("session audit update failed", api.reconciliations[-1][1])

    async def test_unconfirmed_cancellation_enters_reconciliation(self):
        api = FakeAPI()
        gateway = FakeGateway([])
        worker = GatewayWorker(settings(), api, gateway)
        worker.cancelled.add("task-1")
        worker._stop_and_wait = AsyncMock(return_value=None)

        await worker.run_task(task())

        self.assertEqual([], api.reports)
        self.assertIn("cancellation could not be confirmed", api.reconciliations[-1][1])

    async def test_unconfirmed_initial_audit_does_not_requeue(self):
        api = FakeAPI()
        request = httpx.Request("POST", "http://coordinator")
        api.attach_error = httpx.ConnectError("offline", request=request)
        gateway = FakeGateway([])
        worker = GatewayWorker(settings(), api, gateway)
        worker._stop_and_wait = AsyncMock(return_value=None)

        await worker.run_task(task())

        self.assertEqual([], api.reports)
        self.assertIn("binding is unconfirmed", api.reconciliations[-1][1])

    async def test_reconcile_terminal_failure_reports_bound_run(self):
        api = FakeAPI()
        gateway = FakeGateway(
            [{"status": "failed"}, {"status": "succeeded", "result": "new"}]
        )
        await GatewayWorker(settings(), api, gateway).run_task(
            task(remote_run_id="old-run", remote_run_attempt=2)
        )
        self.assertEqual([], gateway.starts)
        self.assertEqual("failed", api.reports[-1][1])
        self.assertTrue(api.reports[-1][4])
        self.assertEqual("old-run", api.reports[-1][5])

    async def test_remote_timed_out_terminal_is_preserved(self):
        api = FakeAPI()
        worker = GatewayWorker(settings(), api, FakeGateway([]))
        worker.claim_tokens["task-1"] = "a" * 32

        await worker._report_gateway_terminal(
            "task-1", "run-1", {"status": "timed_out", "error": "remote deadline"}
        )

        self.assertEqual("timed_out", api.reports[-1][1])
        self.assertFalse(api.reports[-1][4])

    async def test_failed_terminal_after_deadline_is_not_rewritten_as_timeout(self):
        api = FakeAPI()
        gateway = FakeGateway([{"status": "failed", "error": "remote failed"}])

        await GatewayWorker(settings(), api, gateway).run_task(task(timeout_seconds=0))

        self.assertEqual("failed", api.reports[-1][1])
        self.assertEqual("remote failed", api.reports[-1][3])

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
