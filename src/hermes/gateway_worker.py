"""Headless Coordinator worker that executes tasks through one Hermes Gateway."""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Any, Dict, Optional

import httpx

from .config import GatewayWorkerSettings
from .gateway_adapter import GatewayAdapter
from .worker import WorkerAPI, WorkerNotRegisteredError

LOGGER = logging.getLogger("hermes.gateway_worker")
TERMINAL = {
    "succeeded",
    "failed",
    "cancelled",
    "interrupted",
    "timed_out",
    "completed",
    "error",
}
GATEWAY_KINDS = {"local", "remote", "ssh", "cloud"}


class GatewayWorker:
    def __init__(
        self, settings: GatewayWorkerSettings, api: WorkerAPI, gateway: GatewayAdapter
    ) -> None:
        self.settings, self.api, self.gateway = settings, api, gateway
        self.active: Dict[str, asyncio.Task[None]] = {}
        self.claim_tokens: Dict[str, str] = {}
        self.cancelled: set[str] = set()
        self.stopping = asyncio.Event()
        self._registration_lock = asyncio.Lock()
        if not settings.worker_id or not settings.shared_secret:
            raise ValueError("gateway worker id and shared secret are required")
        if not settings.gateway_id:
            raise ValueError("gateway id is required")
        if settings.default_agent != "hermes":
            raise ValueError("M1 Gateway Worker only supports execution agent 'hermes'")
        if settings.concurrency < 1:
            raise ValueError("worker concurrency must be at least 1")
        if settings.gateway_kind not in GATEWAY_KINDS:
            raise ValueError("gateway kind must be one of local, remote, ssh, or cloud")

    async def _attach(
        self, task_id: str, run_id: str, session_id: Optional[str] = None
    ) -> None:
        # This endpoint is deliberately a small coordinator-side audit hook. It
        # is safe to repeat after a worker restart.
        await self.api._request(
            "POST",
            f"/api/v1/tasks/{task_id}/remote-run",
            {
                "claim_token": self.claim_tokens[task_id],
                "remote_run_id": run_id,
                "remote_session_id": session_id,
            },
        )

    async def _profile(self, task: Dict[str, Any]) -> str:
        requested = task.get("resolved_profile")
        if not requested:
            raise ValueError("Coordinator claim did not resolve a Gateway profile")
        if self.settings.profiles and requested not in self.settings.profiles:
            raise ValueError(f"resolved profile is not served: {requested}")
        return requested

    @staticmethod
    def _status(data: Dict[str, Any]) -> str:
        return str(data.get("status", data.get("state", ""))).lower()

    @staticmethod
    def _session_id(data: Dict[str, Any]) -> Optional[str]:
        value = data.get("session_id", data.get("sessionId"))
        return str(value) if value else None

    async def _stop_and_wait(
        self, profile: str, run_id: str
    ) -> Optional[Dict[str, Any]]:
        run = await self.gateway.stop_run(profile, run_id)
        deadline = (
            asyncio.get_running_loop().time() + self.settings.gateway_stop_wait_seconds
        )
        while self._status(run) not in TERMINAL:
            run = await self.gateway.get_run(profile, run_id)
            if self._status(run) in TERMINAL:
                return run
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(self.settings.gateway_poll_interval_seconds)
        return run

    async def _best_effort_stop(
        self, profile: str, run_id: str, reason: str
    ) -> Optional[Dict[str, Any]]:
        try:
            return await self._stop_and_wait(profile, run_id)
        except httpx.HTTPError:
            LOGGER.exception("could not stop Gateway run %s after %s", run_id, reason)
            return None

    async def _handle_binding_conflict(
        self, task_id: str, profile: str, run_id: str
    ) -> None:
        await self._best_effort_stop(profile, run_id, "binding conflict")
        try:
            await self.api.report(
                task_id,
                self.claim_tokens[task_id],
                "cancelled",
                error="Coordinator no longer accepts remote run audit updates",
            )
        except httpx.HTTPError:
            LOGGER.warning(
                "Coordinator also rejected cancellation for stale task %s", task_id
            )

    async def _progress(
        self, task_id: str, phase: str, message: Optional[str] = None
    ) -> None:
        reporter = getattr(self.api, "progress", None)
        if reporter is None:
            return
        try:
            await reporter(task_id, self.claim_tokens[task_id], phase, message)
        except httpx.HTTPError:
            LOGGER.warning("could not report progress for Gateway task %s", task_id)

    async def _mark_reconciling(
        self, task_id: str, reason: str, deadline_exceeded: bool = False
    ) -> None:
        reporter = getattr(self.api, "reconcile", None)
        if reporter is None:
            LOGGER.error("Coordinator client cannot mark task %s reconciling", task_id)
            return
        try:
            await reporter(
                task_id,
                self.claim_tokens[task_id],
                reason,
                deadline_exceeded,
            )
        except httpx.HTTPError:
            LOGGER.warning("could not mark Gateway task %s reconciling", task_id)

    async def _report_remote(
        self,
        task_id: str,
        run_id: str,
        status: str,
        *,
        result: Optional[str] = None,
        error: Optional[str] = None,
        retryable: bool = False,
    ) -> None:
        await self.api.report(
            task_id,
            self.claim_tokens[task_id],
            status,
            result=result,
            error=error,
            retryable=retryable,
            remote_run_id=run_id,
        )

    async def _report_gateway_terminal(
        self, task_id: str, run_id: str, run: Dict[str, Any]
    ) -> None:
        status = self._status(run)
        result = run.get("result", run.get("output"))
        error = run.get("error")
        if status in {"succeeded", "completed"}:
            await self._report_remote(
                task_id,
                run_id,
                "succeeded",
                result=str(result) if result is not None else "",
            )
        elif status == "cancelled":
            await self._report_remote(
                task_id,
                run_id,
                "cancelled",
                error=str(error or "Gateway run cancelled"),
            )
        elif status == "timed_out":
            await self._report_remote(
                task_id,
                run_id,
                "timed_out",
                result=str(result) if result else None,
                error=str(error or "Gateway run timed out"),
            )
        else:
            await self._report_remote(
                task_id,
                run_id,
                "failed",
                result=str(result) if result else None,
                error=str(error or f"Gateway run ended with {status}"),
                retryable=True,
            )

    async def run_task(self, task: Dict[str, Any]) -> None:
        task_id = task["id"]
        self.claim_tokens[task_id] = task["claim_token"]
        remote_bound = False
        try:
            requested_agent = (
                task.get("resolved_execution_agent")
                or task.get("execution_agent")
                or self.settings.default_agent
            )
            if requested_agent != self.settings.default_agent:
                raise ValueError(
                    "Gateway Worker cannot execute requested agent "
                    f"{requested_agent!r}; use a Command Worker with an agent command mapping"
                )
            run_id = task.get("remote_run_id")
            attempt = task.get("attempt_count", 1)
            if run_id and task.get("remote_run_attempt") != attempt:
                LOGGER.warning(
                    "ignoring Gateway run %s from an older attempt of %s",
                    run_id,
                    task_id,
                )
                run_id = None
            remote_bound = bool(run_id)
            profile = await self._profile(task)
            try:
                await self.api.set_running(task_id, self.claim_tokens[task_id])
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 409:
                    LOGGER.warning(
                        "Coordinator rejected stale Gateway claim for %s", task_id
                    )
                    return
                raise
            await self._progress(task_id, "preparing", "preparing Gateway execution")
            prompt = task.get("execution_prompt") or task["prompt"]
            attached_session_id = task.get("remote_session_id")
            if run_id:
                try:
                    run = await self.gateway.get_run(profile, run_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        if task.get("deadline_exceeded_at"):
                            await self._mark_reconciling(
                                task_id,
                                "remote run is not visible after execution deadline",
                                deadline_exceeded=True,
                            )
                            return
                        run_id = None
                        remote_bound = False
                    else:
                        await self._mark_reconciling(
                            task_id, f"Gateway reconciliation failed: {exc}"
                        )
                        return
                except httpx.HTTPError as exc:
                    await self._mark_reconciling(
                        task_id, f"Gateway reconciliation failed: {exc}"
                    )
                    return
                else:
                    previous_status = self._status(run)
                    if previous_status in TERMINAL:
                        await self._report_gateway_terminal(task_id, str(run_id), run)
                        return
            else:
                run = {}
            if task.get("cancel_requested_at") and run_id:
                await self._progress(
                    task_id, "stopping", "stopping remote run after cancellation"
                )
                try:
                    stopped = await self._stop_and_wait(profile, str(run_id))
                except httpx.HTTPError as exc:
                    await self._mark_reconciling(
                        task_id, f"remote cancellation could not be confirmed: {exc}"
                    )
                    return
                if stopped is None:
                    await self._mark_reconciling(
                        task_id, "remote cancellation could not be confirmed"
                    )
                    return
                await self._report_remote(
                    task_id,
                    str(run_id),
                    "cancelled",
                    error="cancelled by director",
                )
                return
            if not run_id:
                await self._progress(
                    task_id, "starting_remote_run", "starting remote Gateway run"
                )
                run = await self.gateway.start_run(
                    profile, prompt, f"{task_id}-{attempt}"
                )
                run_id = run.get("run_id", run.get("id"))
                if not run_id:
                    raise ValueError("Gateway start response has no run id")
                remote_bound = True
                try:
                    await self._attach(
                        task_id,
                        str(run_id),
                        self._session_id(run),
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 409:
                        await self._handle_binding_conflict(
                            task_id, profile, str(run_id)
                        )
                        return
                    stopped = await self._best_effort_stop(
                        profile, str(run_id), "remote-run audit failure"
                    )
                    if stopped is None:
                        await self._mark_reconciling(
                            task_id,
                            f"remote run started but audit binding failed: {exc}",
                        )
                        return
                    await self.api.report(
                        task_id,
                        self.claim_tokens[task_id],
                        "failed",
                        error=f"remote run audit failed after run was stopped: {exc}",
                        retryable=True,
                    )
                    return
                except httpx.HTTPError as exc:
                    stopped = await self._best_effort_stop(
                        profile, str(run_id), "remote-run audit failure"
                    )
                    if stopped is None:
                        try:
                            await self._attach(
                                task_id,
                                str(run_id),
                                self._session_id(run),
                            )
                        except httpx.HTTPError:
                            pass
                        await self._mark_reconciling(
                            task_id,
                            f"remote run started but audit binding is unconfirmed: {exc}",
                        )
                        return
                    try:
                        await self.api.report(
                            task_id,
                            self.claim_tokens[task_id],
                            "failed",
                            error=(
                                "remote run audit failed after the run was "
                                f"confirmed stopped: {exc}"
                            ),
                            retryable=True,
                        )
                    except httpx.HTTPError:
                        await self._mark_reconciling(
                            task_id,
                            "remote run audit outcome is ambiguous after a transport failure",
                        )
                    return
                attached_session_id = self._session_id(run)
            await self._progress(
                task_id, "remote_running", f"remote run {run_id} is active"
            )
            timeout_seconds = min(
                int(task.get("timeout_seconds", self.settings.task_timeout_seconds)),
                self.settings.task_timeout_seconds,
            )
            started_at = float(task.get("started_at") or time.time())
            remaining = max(0.0, started_at + timeout_seconds - time.time())
            deadline = asyncio.get_running_loop().time() + remaining
            while True:
                status = self._status(run)
                if task_id in self.cancelled:
                    if status not in TERMINAL:
                        stopped = await self._stop_and_wait(profile, str(run_id))
                        if stopped is None:
                            await self._mark_reconciling(
                                task_id,
                                "remote cancellation could not be confirmed",
                            )
                            return
                    await self._report_remote(
                        task_id,
                        str(run_id),
                        "cancelled",
                        error="cancelled by director",
                    )
                    return
                session_id = self._session_id(run)
                if session_id and session_id != attached_session_id:
                    try:
                        await self._attach(task_id, str(run_id), session_id)
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 409:
                            stopped = await self._best_effort_stop(
                                profile, str(run_id), "remote cancellation race"
                            )
                            if stopped is None:
                                await self._mark_reconciling(
                                    task_id,
                                    "remote cancellation could not be confirmed",
                                )
                                return
                            await self._report_remote(
                                task_id,
                                str(run_id),
                                "cancelled",
                                error="cancelled by director",
                            )
                            return
                        raise
                    except httpx.HTTPError as exc:
                        await self._mark_reconciling(
                            task_id,
                            f"remote session audit update failed: {exc}",
                        )
                        return
                    attached_session_id = session_id
                if status in TERMINAL:
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    await self._progress(
                        task_id,
                        "stopping",
                        "execution deadline reached; stopping remote run",
                    )
                    try:
                        stopped = await self._stop_and_wait(profile, str(run_id))
                    except httpx.HTTPError as exc:
                        await self._mark_reconciling(
                            task_id,
                            f"execution deadline reached; stop unconfirmed: {exc}",
                            deadline_exceeded=True,
                        )
                        return
                    if stopped is None:
                        await self._mark_reconciling(
                            task_id,
                            "execution deadline reached; stop unconfirmed",
                            deadline_exceeded=True,
                        )
                        return
                    stopped_status = self._status(stopped)
                    if stopped_status in {"cancelled", "interrupted"}:
                        await self._report_remote(
                            task_id,
                            str(run_id),
                            "timed_out",
                            error="Gateway execution timed out",
                        )
                    else:
                        await self._report_gateway_terminal(
                            task_id, str(run_id), stopped
                        )
                    return
                await asyncio.sleep(self.settings.gateway_poll_interval_seconds)
                try:
                    run = await self.gateway.get_run(profile, str(run_id))
                except httpx.HTTPError as exc:
                    await self._mark_reconciling(
                        task_id, f"Gateway status polling failed: {exc}"
                    )
                    return
            await self._progress(
                task_id, "collecting_result", "collecting Gateway result"
            )
            await self._report_gateway_terminal(task_id, str(run_id), run)
        except httpx.HTTPError as exc:
            LOGGER.exception("Gateway communication failed for %s", task_id)
            if remote_bound:
                await self._mark_reconciling(
                    task_id, f"Gateway communication failed: {exc}"
                )
            else:
                try:
                    await self.api.report(
                        task_id,
                        self.claim_tokens[task_id],
                        "failed",
                        error=f"Gateway communication failed before run binding: {exc}",
                        retryable=True,
                    )
                except Exception:
                    LOGGER.exception("could not report Gateway failure")
        except Exception as exc:
            LOGGER.exception("Gateway task failed: %s", task_id)
            if remote_bound:
                await self._mark_reconciling(
                    task_id, f"Gateway worker lost control of remote run: {exc}"
                )
            else:
                try:
                    await self.api.report(
                        task_id,
                        self.claim_tokens[task_id],
                        "failed",
                        error=str(exc),
                        retryable=False,
                    )
                except Exception:
                    LOGGER.exception("could not report task failure")
        finally:
            self.cancelled.discard(task_id)
            self.claim_tokens.pop(task_id, None)

    async def heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                running_claims = [
                    {"task_id": task_id, "claim_token": claim_token}
                    for task_id, claim_token in self.claim_tokens.items()
                    if task_id in self.active
                ]
                ids = await self.api.heartbeat(running_claims)
                self.cancelled.update(ids)
            except WorkerNotRegisteredError:
                await self._recover_registration()
            except Exception:
                LOGGER.exception("heartbeat failed")
            try:
                await asyncio.wait_for(
                    self.stopping.wait(), self.settings.heartbeat_interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _register(self) -> None:
        discovered = self.settings.profiles or await self.gateway.discover_profiles()
        profiles = list(
            dict.fromkeys(profile.strip() for profile in discovered if profile.strip())
        )
        if not profiles:
            raise ValueError("Gateway Worker requires at least one non-blank profile")
        supported_agents = ["hermes"]
        routes = [
            {
                "route_id": f"{self.settings.gateway_id}:{profile}",
                "gateway_id": self.settings.gateway_id,
                "profile": profile,
                "target_profile": profile,
                "gateway_kind": self.settings.gateway_kind,
                "supported_agents": supported_agents,
                "default_agent": self.settings.default_agent,
            }
            for profile in profiles
        ]
        await self.api.register(
            self.settings.worker_name,
            self.settings.concurrency,
            capabilities=["worker-kind:gateway"]
            + [f"agent:{agent}" for agent in supported_agents]
            + [f"profile:{p}" for p in profiles],
            metadata={
                "worker_kind": "gateway",
                "gateway_id": self.settings.gateway_id,
                "profiles": profiles,
                "default_agent": self.settings.default_agent,
            },
            worker_kind="gateway",
            default_agent=self.settings.default_agent,
            routes=routes,
        )

    async def _recover_registration(self) -> bool:
        LOGGER.warning(
            "worker %s is no longer registered; re-registering",
            self.settings.worker_id,
        )
        try:
            async with self._registration_lock:
                await self._register()
        except Exception:
            LOGGER.exception(
                "worker %s re-registration failed", self.settings.worker_id
            )
            return False
        LOGGER.info("worker %s re-registered", self.settings.worker_id)
        return True

    async def run(self, once: bool = False) -> None:
        await self._register()
        hb = asyncio.create_task(self.heartbeat_loop())
        try:
            while not self.stopping.is_set():
                claimed = False
                while len(self.active) < self.settings.concurrency:
                    try:
                        task = await self.api.claim()
                    except httpx.ReadTimeout:
                        LOGGER.warning("claim request timed out; retrying")
                        continue
                    except WorkerNotRegisteredError:
                        if await self._recover_registration():
                            continue
                        break
                    if not task:
                        break
                    claimed = True
                    job = asyncio.create_task(self.run_task(task))
                    self.active[task["id"]] = job
                    job.add_done_callback(
                        lambda done, tid=task["id"]: self.active.pop(tid, None)
                    )
                if once and not self.active and not claimed:
                    break
                await asyncio.sleep(self.settings.poll_interval_seconds)
        finally:
            self.stopping.set()
            hb.cancel()
            await asyncio.gather(hb, return_exceptions=True)
            if self.active:
                await asyncio.gather(*self.active.values(), return_exceptions=True)


async def run_gateway_worker(
    settings: GatewayWorkerSettings, once: bool = False
) -> None:
    # Compatibility entrypoint: the deployed runtime is now the same unified
    # control loop used by ``hermes-worker``.
    from .config import UnifiedWorkerSettings
    from .worker import run_unified_worker

    unified_settings = UnifiedWorkerSettings(
        coordinator_url=settings.coordinator_url,
        worker_id=settings.worker_id,
        worker_name=settings.worker_name,
        shared_secret=settings.shared_secret,
        default_agent="hermes",
        agent_commands={"hermes": ["hermes", "chat", "-q", "{prompt}"]},
        gateway_url=settings.gateway_url,
        gateway_id=settings.gateway_id,
        gateway_kind=settings.gateway_kind,
        gateway_token=settings.gateway_token,
        profile_keys=settings.profile_keys,
        profiles=settings.profiles,
        default_profile=settings.default_profile,
        concurrency=settings.concurrency,
        task_timeout_seconds=settings.task_timeout_seconds,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        poll_interval_seconds=settings.poll_interval_seconds,
        gateway_poll_interval_seconds=settings.gateway_poll_interval_seconds,
        gateway_request_timeout_seconds=settings.gateway_request_timeout_seconds,
        gateway_stop_wait_seconds=settings.gateway_stop_wait_seconds,
    )
    await run_unified_worker(unified_settings, once=once)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a headless Hermes Gateway Worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_gateway_worker(GatewayWorkerSettings.from_env(), args.once))


if __name__ == "__main__":
    main()
