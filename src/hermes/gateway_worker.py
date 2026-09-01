"""Headless Coordinator worker that executes tasks through one Hermes Gateway."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any, Dict, Optional

import httpx

from .config import GatewayWorkerSettings
from .gateway_adapter import GatewayAdapter
from .worker import WorkerAPI

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
        self.cancelled: set[str] = set()
        self.stopping = asyncio.Event()
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
            {"remote_run_id": run_id, "remote_session_id": session_id},
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
                "cancelled",
                error="Coordinator no longer accepts remote run audit updates",
            )
        except httpx.HTTPError:
            LOGGER.warning(
                "Coordinator also rejected cancellation for stale task %s", task_id
            )

    async def run_task(self, task: Dict[str, Any]) -> None:
        task_id = task["id"]
        try:
            profile = await self._profile(task)
            try:
                await self.api.set_running(task_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 409:
                    LOGGER.warning(
                        "Coordinator rejected stale Gateway claim for %s", task_id
                    )
                    return
                raise
            prompt = task.get("execution_prompt") or task["prompt"]
            attempt = task.get("attempt_count", 1)
            run_id = task.get("remote_run_id")
            attached_session_id = task.get("remote_session_id")
            if run_id:
                try:
                    run = await self.gateway.get_run(profile, run_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        run_id = None
                    else:
                        await self._best_effort_stop(
                            profile, str(run_id), "reconcile failure"
                        )
                        raise
                except httpx.HTTPError:
                    await self._best_effort_stop(
                        profile, str(run_id), "reconcile failure"
                    )
                    raise
                else:
                    previous_status = self._status(run)
                    if previous_status in TERMINAL and previous_status not in {
                        "succeeded",
                        "completed",
                    }:
                        run_id = None
                        run = {}
            else:
                run = {}
            if not run_id:
                run = await self.gateway.start_run(
                    profile, prompt, f"{task_id}-{attempt}"
                )
                run_id = run.get("run_id", run.get("id"))
                if not run_id:
                    raise ValueError("Gateway start response has no run id")
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
                    await self._best_effort_stop(
                        profile, str(run_id), "remote-run audit failure"
                    )
                    raise
                except httpx.HTTPError:
                    await self._best_effort_stop(
                        profile, str(run_id), "remote-run audit failure"
                    )
                    raise
                attached_session_id = self._session_id(run)
            deadline = asyncio.get_running_loop().time() + min(
                int(task.get("timeout_seconds", self.settings.task_timeout_seconds)),
                self.settings.task_timeout_seconds,
            )
            while True:
                session_id = self._session_id(run)
                if session_id and session_id != attached_session_id:
                    try:
                        await self._attach(task_id, str(run_id), session_id)
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 409:
                            await self._handle_binding_conflict(
                                task_id, profile, str(run_id)
                            )
                            return
                        await self._best_effort_stop(
                            profile, str(run_id), "session audit failure"
                        )
                        raise
                    attached_session_id = session_id
                status = self._status(run)
                if task_id in self.cancelled:
                    if status not in TERMINAL:
                        stopped = await self._stop_and_wait(profile, str(run_id))
                        if stopped is None:
                            LOGGER.error(
                                "Gateway run %s did not stop before the grace deadline",
                                run_id,
                            )
                            return
                    await self.api.report(
                        task_id, "cancelled", error="cancelled by director"
                    )
                    return
                if status in TERMINAL:
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    stopped = await self._stop_and_wait(profile, str(run_id))
                    if stopped is None:
                        LOGGER.error(
                            "Timed-out Gateway run %s did not stop before the grace deadline",
                            run_id,
                        )
                        return
                    await self.api.report(
                        task_id, "timed_out", error="Gateway execution timed out"
                    )
                    return
                await asyncio.sleep(self.settings.gateway_poll_interval_seconds)
                try:
                    run = await self.gateway.get_run(profile, str(run_id))
                except httpx.HTTPError:
                    await self._best_effort_stop(
                        profile, str(run_id), "status polling failure"
                    )
                    raise
            result = run.get("result", run.get("output"))
            error = run.get("error")
            if status in {"succeeded", "completed"}:
                await self.api.report(
                    task_id,
                    "succeeded",
                    result=str(result) if result is not None else "",
                )
            elif status == "cancelled":
                await self.api.report(
                    task_id, "cancelled", error=str(error or "Gateway run cancelled")
                )
            else:
                await self.api.report(
                    task_id,
                    "failed",
                    result=str(result) if result else None,
                    error=str(error or f"Gateway run ended with {status}"),
                    retryable=True,
                )
        except httpx.HTTPError as exc:
            LOGGER.exception("Gateway communication failed for %s", task_id)
            try:
                await self.api.report(
                    task_id,
                    "failed",
                    error=f"Gateway communication failed: {exc}",
                    retryable=True,
                )
            except Exception:
                LOGGER.exception("could not report Gateway failure")
        except Exception as exc:
            LOGGER.exception("Gateway task failed: %s", task_id)
            try:
                await self.api.report(
                    task_id, "failed", error=str(exc), retryable=False
                )
            except Exception:
                LOGGER.exception("could not report task failure")
        finally:
            self.cancelled.discard(task_id)

    async def heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                ids = await self.api.heartbeat(list(self.active))
                self.cancelled.update(ids)
            except Exception:
                LOGGER.exception("heartbeat failed")
            try:
                await asyncio.wait_for(
                    self.stopping.wait(), self.settings.heartbeat_interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def run(self, once: bool = False) -> None:
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
        hb = asyncio.create_task(self.heartbeat_loop())
        try:
            while not self.stopping.is_set():
                claimed = False
                while len(self.active) < self.settings.concurrency:
                    task = await self.api.claim()
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
    api = WorkerAPI(
        settings.coordinator_url, settings.worker_id, settings.shared_secret
    )
    gateway = GatewayAdapter(
        settings.gateway_url,
        settings.gateway_token,
        settings.profile_keys,
        default_profile=settings.default_profile,
        configured_profiles=settings.profiles,
        timeout=settings.gateway_request_timeout_seconds,
    )
    try:
        await GatewayWorker(settings, api, gateway).run(once)
    finally:
        await api.close()
        await gateway.close()


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
