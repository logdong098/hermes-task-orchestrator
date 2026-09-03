"""Protocol helpers for the external Codex with ChatGPT planning workflow.

Hermes deliberately does not start ``codex exec`` here.  The
``codex-with-chatgpt`` Skill is owned by the active Codex session and uses its
ChatGPT planning/review conversation.  This module exposes the small HTTP
client and message builders that let that Codex controller claim a task and
return its plan to Hermes.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

C2C_PLANNER_AGENT = "codex-with-chatgpt"
C2C_PROTOCOL_VERSION = "1"
MAX_CONTROL_MESSAGE_BYTES = 900


def build_c2c_init_message(task: Dict[str, Any]) -> str:
    """Build the compact INIT control message for the C2C conversation.

    The full task remains in Hermes. Only a bounded goal summary is included
    in the control message so task text cannot turn into a large data dump.
    The connected workspace is the source of truth for code and files.
    """

    task_id = str(task.get("planner_task_id") or task["id"])
    goal = " ".join(str(task["prompt"]).split())
    prefix = f"[C2C]\nSTATE: INIT\nTASK_ID: {task_id}\nITERATION: 0\n\nGOAL:\n"
    suffix = (
        "\n\nINSTRUCTION:\n"
        "Use the codex-with-chatgpt Skill to inspect the connected workspace and "
        "return a C2C PLAN for a Claude Code worker."
    )
    available = MAX_CONTROL_MESSAGE_BYTES - len((prefix + suffix).encode("utf-8"))
    goal_bytes = goal.encode("utf-8")
    if available < len(goal_bytes):
        goal = (
            goal_bytes[: max(0, available - 3)].decode("utf-8", errors="ignore") + "..."
        )
    return prefix + goal + suffix


def build_execution_prompt(prompt: str, plan: str) -> str:
    """Combine the user request and ChatGPT's plan for the execution Worker."""

    return (
        "Original development task:\n"
        f"{prompt}\n\n"
        "Codex with ChatGPT execution plan:\n"
        f"{plan}\n\n"
        "Implement the task in the assigned workspace and verify the result."
    )


class CodexWithChatGPTAPI:
    """Small Director-authenticated API used by the active Codex controller.

    This client intentionally stops at the Hermes boundary. Sending INIT and
    receiving PLAN happens in the Codex with ChatGPT Skill; Hermes only queues
    the request and accepts the resulting plan.
    """

    def __init__(
        self,
        base_url: str,
        director_api_key: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.director_api_key = director_api_key
        self.client = client or httpx.AsyncClient(timeout=30)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        response = await self.client.request(
            method,
            f"{self.base_url}{path}",
            json=payload,
            headers={"Authorization": f"Bearer {self.director_api_key}"},
        )
        response.raise_for_status()
        if not response.content:
            return None
        return response.json()

    async def claim_task(self) -> Optional[Dict[str, Any]]:
        response = await self._request("POST", "/api/v1/planner/tasks/claim")
        return response.get("task") if response else None

    async def submit_plan(
        self,
        task: Dict[str, Any],
        plan: str,
        execution_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "planner_claim_token": task["planner_claim_token"],
            "plan": plan,
        }
        if execution_prompt is not None:
            payload["execution_prompt"] = execution_prompt
        return await self._request("POST", f"/api/v1/tasks/{task['id']}/plan", payload)

    async def fail_task(
        self, task: Dict[str, Any], error: str, retryable: bool = True
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/v1/tasks/{task['id']}/planning-failure",
            {
                "planner_claim_token": task["planner_claim_token"],
                "error": error,
                "retryable": retryable,
            },
        )
