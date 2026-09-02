from __future__ import annotations

import argparse
import asyncio
import hmac
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status

from .config import CoordinatorSettings
from .models import (
    HeartbeatRequest,
    NotificationResponse,
    PlanningMode,
    TaskCreate,
    TaskEventResponse,
    TaskProgressUpdate,
    TaskReconcileRequest,
    TaskRemoteRunUpdate,
    TaskResponse,
    TaskResult,
    TaskStatusUpdate,
    WorkerRegistration,
    WorkerResponse,
    WorkerRouteResponse,
)
from .planner import PlannerRuntime, PlannerSettings
from .security import verify_signature
from .storage import ConflictError, NotFoundError, SQLiteStore

LOGGER = logging.getLogger("hermes.coordinator")


def create_app(
    settings: Optional[CoordinatorSettings] = None,
    store: Optional[SQLiteStore] = None,
) -> FastAPI:
    configured = settings or CoordinatorSettings.from_env()
    repository = store or SQLiteStore(configured.database_path)
    repository.reconciliation_grace_seconds = configured.reconciliation_grace_seconds
    repository.reconciliation_backoff_seconds = (
        configured.reconciliation_backoff_seconds
    )
    planner = PlannerRuntime(
        PlannerSettings(
            agent_commands=configured.planner_commands,
            default_agent=configured.default_planner_agent,
            timeout_seconds=configured.planner_timeout_seconds,
            max_output_bytes=configured.planner_max_output_bytes,
            poll_interval_seconds=configured.planner_poll_interval_seconds,
            lease_seconds=configured.planner_lease_seconds,
        ),
        repository,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        repository.initialize()
        stopping = asyncio.Event()

        async def maintenance_loop() -> None:
            while not stopping.is_set():
                try:
                    repository.run_maintenance()
                except Exception:
                    LOGGER.exception("task maintenance failed")
                try:
                    await asyncio.wait_for(
                        stopping.wait(),
                        timeout=max(configured.maintenance_interval_seconds, 0.1),
                    )
                except asyncio.TimeoutError:
                    pass

        maintenance_task = asyncio.create_task(maintenance_loop())
        planner_task = asyncio.create_task(planner.run(stopping))
        try:
            yield
        finally:
            stopping.set()
            for task_id in list(planner.processes):
                await planner.cancel_task(task_id)
            planner_task.cancel()
            await asyncio.gather(planner_task, return_exceptions=True)
            await maintenance_task

    app = FastAPI(
        title="Hermes Coordinator",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = configured
    app.state.store = repository
    app.state.planner = planner

    async def worker_auth(
        request: Request,
        x_hermes_worker_id: str = Header(default=""),
        x_hermes_timestamp: str = Header(default=""),
        x_hermes_nonce: str = Header(default=""),
        x_hermes_signature: str = Header(default=""),
    ) -> str:
        if not x_hermes_worker_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing worker id",
            )
        worker_secret = configured.worker_secret_for(x_hermes_worker_id)
        if not worker_secret:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="worker authentication is not configured",
            )
        body = await request.body()
        if not verify_signature(
            worker_secret,
            request.method,
            request.url.path,
            body,
            x_hermes_timestamp,
            x_hermes_nonce,
            x_hermes_signature,
            configured.hmac_max_clock_skew_seconds,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid worker signature",
            )
        if not repository.consume_worker_nonce(
            x_hermes_worker_id,
            x_hermes_nonce,
            configured.hmac_max_clock_skew_seconds * 2,
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="worker request nonce was already used",
            )
        return x_hermes_worker_id

    def director_auth(authorization: str = Header(default="")) -> None:
        if not configured.director_api_key:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="director authentication is not configured",
            )
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            token, configured.director_api_key
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid director token",
            )

    def task_or_404(task_id: str) -> Dict[str, Any]:
        try:
            task = with_routing_diagnostic(repository.get_task(task_id))
            task["recent_events"] = repository.list_task_events(task_id, limit=8)
            return task
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    def with_routing_diagnostic(task: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(task)
        enriched["routing_diagnostic"] = None
        if task.get("status") != "pending" or not task.get("target_gateway_id"):
            return enriched
        routes = repository.list_routes(configured.worker_stale_seconds)
        matching = [
            route
            for route in routes
            if route.get("gateway_id") == task.get("target_gateway_id")
            and (route.get("target_profile") or route.get("profile"))
            == task.get("target_profile")
        ]
        if task.get("target_worker_id"):
            matching = [
                route
                for route in matching
                if route.get("worker_id") == task.get("target_worker_id")
            ]
        if not matching:
            enriched["routing_diagnostic"] = (
                "no registered route matches the requested Gateway/Profile"
            )
            return enriched
        online = [route for route in matching if route.get("status") == "online"]
        if not online:
            enriched["routing_diagnostic"] = "matching route is offline"
            return enriched
        requested_agent = task.get("execution_agent")
        if requested_agent and not any(
            requested_agent in route.get("supported_agents", []) for route in online
        ):
            enriched["routing_diagnostic"] = (
                "matching route does not support the requested execution agent"
            )
            return enriched
        enriched["routing_diagnostic"] = (
            "matching route is online but currently has no claim capacity"
        )
        return enriched

    @app.get("/healthz")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/workers/register", response_model=WorkerResponse)
    def register_worker(
        registration: WorkerRegistration,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        if registration.worker_id != authenticated_worker_id:
            raise HTTPException(status_code=403, detail="worker id mismatch")
        return repository.register_worker(registration.model_dump())

    @app.post("/api/v1/workers/{worker_id}/heartbeat")
    def heartbeat(
        worker_id: str,
        heartbeat_request: HeartbeatRequest,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        if worker_id != authenticated_worker_id:
            raise HTTPException(status_code=403, detail="worker id mismatch")
        try:
            cancel_task_ids = repository.heartbeat(
                worker_id,
                heartbeat_request.running_task_ids,
                configured.task_lease_seconds,
                routes=(
                    [
                        route.model_dump(mode="json")
                        for route in heartbeat_request.routes
                    ]
                    if heartbeat_request.routes is not None
                    else None
                ),
                running_claims=[
                    claim.model_dump() for claim in heartbeat_request.running_claims
                ],
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"cancel_task_ids": cancel_task_ids}

    @app.get(
        "/api/v1/workers",
        response_model=List[WorkerResponse],
        dependencies=[Depends(director_auth)],
    )
    def list_workers() -> List[Dict[str, Any]]:
        return repository.list_workers(configured.worker_stale_seconds)

    @app.get(
        "/api/v1/routes",
        response_model=List[WorkerRouteResponse],
        dependencies=[Depends(director_auth)],
    )
    def list_routes() -> List[Dict[str, Any]]:
        return repository.list_routes(configured.worker_stale_seconds)

    @app.post(
        "/api/v1/tasks",
        response_model=TaskResponse,
        status_code=status.HTTP_201_CREATED,
        dependencies=[Depends(director_auth)],
    )
    def create_task(task: TaskCreate) -> Dict[str, Any]:
        payload = task.model_dump(mode="json")
        requested_mode = task.planning_mode
        resolved_mode = (
            PlanningMode.PLAN if requested_mode == PlanningMode.AUTO else requested_mode
        )
        payload["planning_mode"] = resolved_mode.value
        payload["planner_agent"] = (
            payload.get("planner_agent") or configured.default_planner_agent
            if resolved_mode == PlanningMode.PLAN
            else None
        )
        return repository.create_task(
            payload,
            configured.default_task_timeout_seconds,
            configured.max_task_timeout_seconds,
            configured.default_max_attempts,
            configured.planner_max_attempts,
        )

    @app.get(
        "/api/v1/tasks",
        response_model=List[TaskResponse],
        dependencies=[Depends(director_auth)],
    )
    def list_tasks(limit: int = 100) -> List[Dict[str, Any]]:
        return [
            with_routing_diagnostic(task)
            for task in repository.list_tasks(max(1, min(limit, 500)))
        ]

    @app.get(
        "/api/v1/tasks/{task_id}",
        response_model=TaskResponse,
        dependencies=[Depends(director_auth)],
    )
    def get_task(task_id: str) -> Dict[str, Any]:
        return task_or_404(task_id)

    @app.get(
        "/api/v1/tasks/{task_id}/events",
        response_model=List[TaskEventResponse],
        dependencies=[Depends(director_auth)],
    )
    def list_task_events(task_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        try:
            return repository.list_task_events(task_id, limit=limit)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/api/v1/tasks/{task_id}/cancel",
        response_model=TaskResponse,
        dependencies=[Depends(director_auth)],
    )
    async def cancel_task(task_id: str) -> Dict[str, Any]:
        try:
            task = repository.cancel_task(task_id)
            await planner.cancel_task(task_id)
            return task
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/v1/workers/{worker_id}/tasks/claim")
    def claim_task(
        worker_id: str,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        if worker_id != authenticated_worker_id:
            raise HTTPException(status_code=403, detail="worker id mismatch")
        try:
            task = repository.claim_task(worker_id, configured.task_lease_seconds)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"task": task}

    @app.post("/api/v1/tasks/{task_id}/status", response_model=TaskResponse)
    def update_task_status(
        task_id: str,
        update: TaskStatusUpdate,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        try:
            return repository.update_task_status(
                task_id,
                authenticated_worker_id,
                update.status.value,
                claim_token=update.claim_token,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/tasks/{task_id}/progress", response_model=TaskResponse)
    def report_progress(
        task_id: str,
        update: TaskProgressUpdate,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        try:
            return repository.record_progress(
                task_id,
                authenticated_worker_id,
                update.phase,
                update.message,
                update.details,
                claim_token=update.claim_token,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/tasks/{task_id}/reconcile", response_model=TaskResponse)
    def mark_task_reconciling(
        task_id: str,
        update: TaskReconcileRequest,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        try:
            return repository.mark_reconciling(
                task_id,
                authenticated_worker_id,
                update.reason,
                update.deadline_exceeded,
                claim_token=update.claim_token,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/tasks/{task_id}/remote-run", response_model=TaskResponse)
    def attach_remote_run(
        task_id: str,
        update: TaskRemoteRunUpdate,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        try:
            return repository.attach_remote_run(
                task_id,
                authenticated_worker_id,
                update.remote_run_id,
                update.remote_session_id,
                claim_token=update.claim_token,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/tasks/{task_id}/result", response_model=TaskResponse)
    def report_result(
        task_id: str,
        task_result: TaskResult,
        authenticated_worker_id: str = Depends(worker_auth),
    ) -> Dict[str, Any]:
        try:
            return repository.report_result(
                task_id,
                authenticated_worker_id,
                task_result.status.value,
                task_result.result,
                task_result.error,
                task_result.retryable,
                task_result.remote_run_id,
                claim_token=task_result.claim_token,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get(
        "/api/v1/notifications",
        response_model=List[NotificationResponse],
        dependencies=[Depends(director_auth)],
    )
    def list_notifications(limit: int = 100) -> List[Dict[str, Any]]:
        return repository.list_notifications(max(1, min(limit, 500)))

    @app.post(
        "/api/v1/notifications/{notification_id}/ack",
        status_code=status.HTTP_204_NO_CONTENT,
        response_class=Response,
        dependencies=[Depends(director_auth)],
    )
    def acknowledge_notification(notification_id: int) -> Response:
        try:
            repository.acknowledge_notification(notification_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


app = create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Hermes Coordinator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    arguments = parser.parse_args()
    uvicorn.run(
        "hermes.coordinator:app",
        host=arguments.host,
        port=arguments.port,
        log_level=arguments.log_level,
    )


if __name__ == "__main__":
    main()
