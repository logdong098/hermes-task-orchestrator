from __future__ import annotations

import argparse
import asyncio
import logging
import shlex
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .config import TelegramSettings
from .models import canonical_agent_name
from .planner import C2C_PLANNER_AGENT

LOGGER = logging.getLogger("hermes.telegram")
HELP_TEXT = """Hermes Director 命令：
/agents - 查看 Worker 在线状态
/tasks - 查看最近任务与进度
/new [--planner codex-with-chatgpt] [--worker <id>] [--workdir <目录>] [--gateway <id> --profile <name>] [--executor <agent>] <任务> - 创建任务
/status <任务ID> - 查询任务
/cancel <任务ID> - 取消任务
/help - 显示帮助

直接发送普通文本也会创建任务。
@worker [-codex|-cc|--executor/--agent <agent>] <任务> - 使用默认/在线 Worker 直接执行
@worker1 [-codex|-cc|--executor/--agent <agent>] <任务> - 跳过规划，直接交给指定 Worker
@Coordinator [@worker1] [-codex|-cc|--executor/--agent <agent>] <任务> - 先规划，再自动或定向执行"""


class DirectorAPI:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client or httpx.AsyncClient(timeout=30)
        self.api_key = api_key
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
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        response.raise_for_status()
        if not response.content:
            return None
        return response.json()

    async def list_workers(self) -> List[Dict[str, Any]]:
        return await self._request("GET", "/api/v1/workers")

    async def list_tasks(self, limit: int = 20) -> List[Dict[str, Any]]:
        return await self._request("GET", f"/api/v1/tasks?limit={limit}")

    async def create_task(
        self,
        prompt: str,
        user_id: int,
        chat_id: int,
        idempotency_key: Optional[str] = None,
        planner_agent: Optional[str] = None,
        execution_agent: Optional[str] = None,
        target_worker_id: Optional[str] = None,
        workdir: Optional[str] = None,
        target_gateway_id: Optional[str] = None,
        target_profile: Optional[str] = None,
        planning_mode: str = "auto",
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            "/api/v1/tasks",
            {
                "prompt": prompt,
                "creator_user_id": str(user_id),
                "telegram_chat_id": str(chat_id),
                "idempotency_key": idempotency_key,
                "planner_agent": planner_agent,
                "planning_mode": planning_mode,
                "execution_agent": execution_agent,
                "target_worker_id": target_worker_id,
                "workdir": workdir,
                "target_gateway_id": target_gateway_id,
                "target_profile": target_profile,
            },
        )

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/api/v1/tasks/{task_id}")

    async def cancel_task(self, task_id: str) -> Dict[str, Any]:
        return await self._request("POST", f"/api/v1/tasks/{task_id}/cancel")

    async def notifications(self) -> List[Dict[str, Any]]:
        return await self._request("GET", "/api/v1/notifications")

    async def acknowledge(self, notification_id: int) -> None:
        await self._request("POST", f"/api/v1/notifications/{notification_id}/ack")


class TelegramGateway:
    def __init__(
        self,
        token: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.client = client or httpx.AsyncClient(timeout=40)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def get_updates(
        self, offset: Optional[int], timeout_seconds: int
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "timeout": timeout_seconds,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = offset
        response = await self.client.post(f"{self.base_url}/getUpdates", json=payload)
        response.raise_for_status()
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError("Telegram getUpdates returned an error")
        return body.get("result", [])

    async def send_message(self, chat_id: int, text: str) -> None:
        chunks = [
            text[index : index + 4000] for index in range(0, len(text), 4000)
        ] or [""]
        for chunk in chunks:
            response = await self.client.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": chat_id, "text": chunk},
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("ok"):
                raise RuntimeError("Telegram sendMessage returned an error")


class DirectorBot:
    def __init__(
        self,
        settings: TelegramSettings,
        director: DirectorAPI,
        telegram: TelegramGateway,
    ) -> None:
        self.settings = settings
        self.director = director
        self.telegram = telegram
        self.allowed_user_ids = set(settings.allowed_user_ids)
        self.stopping = asyncio.Event()

    async def handle_update(self, update: Dict[str, Any]) -> None:
        message = update.get("message") or {}
        text = message.get("text")
        sender = message.get("from") or {}
        chat = message.get("chat") or {}
        if not text or "id" not in sender or "id" not in chat:
            return
        user_id = int(sender["id"])
        chat_id = int(chat["id"])
        if user_id not in self.allowed_user_ids:
            LOGGER.warning("rejected unauthorized Telegram user %s", user_id)
            await self.telegram.send_message(chat_id, "无权访问此 Director Bot。")
            return
        try:
            update_id = update.get("update_id")
            idempotency_key = (
                f"telegram-update:{update_id}" if update_id is not None else None
            )
            response = await self.handle_text(
                text.strip(), user_id, chat_id, idempotency_key
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                response = "未找到该任务。"
            else:
                LOGGER.exception("coordinator request failed")
                response = "Coordinator 请求失败，请稍后重试。"
        except Exception:
            LOGGER.exception("Telegram command failed")
            response = "处理命令时发生内部错误。"
        await self.telegram.send_message(chat_id, response)

    async def handle_text(
        self,
        text: str,
        user_id: int,
        chat_id: int,
        idempotency_key: Optional[str] = None,
    ) -> str:
        parts = text.split(maxsplit=1)
        command = parts[0]
        argument = parts[1].strip() if len(parts) == 2 else ""
        command = command.split("@", 1)[0].lower()
        if command == "/help" or command == "/start":
            return HELP_TEXT
        if command == "/agents":
            workers = await self.director.list_workers()
            if not workers:
                return "暂无已注册 Worker。"
            lines = ["Worker 列表："]
            for worker in workers:
                lines.append(
                    f"- {worker['worker_id']} | {worker['status']} | "
                    f"并发 {worker['max_concurrency']}"
                )
            return "\n".join(lines)
        if command == "/tasks":
            tasks = await self.director.list_tasks(limit=20)
            if not tasks:
                return "暂无任务。"
            lines = ["最近任务："]
            for task in tasks:
                worker = task.get("resolved_worker_id") or task.get("target_worker_id")
                worker_text = f" | {worker}" if worker else ""
                lines.append(
                    f"- {task['id']} | {task['status']} | "
                    f"{task.get('current_phase') or 'created'}{worker_text}"
                )
            return "\n".join(lines)
        if command == "/new":
            if not argument:
                return (
                    "用法：/new [--planner codex-with-chatgpt] [--worker <id>] "
                    "[--workdir <目录>] [--gateway <id> --profile <name>] "
                    "[--executor <agent>] <任务描述>"
                )
            parsed = self._parse_new_arguments(argument)
            if parsed is None:
                return (
                    "参数错误。Gateway 与 Profile 必须同时指定。\n"
                    "用法：/new [--planner codex-with-chatgpt] [--worker <id>] "
                    "[--workdir <目录>] [--gateway <id> --profile <name>] "
                    "[--executor <agent>] <任务描述>"
                )
            (
                prompt,
                planner_agent,
                execution_agent,
                target_worker_id,
                workdir,
                target_gateway_id,
                target_profile,
            ) = parsed
            task = await self.director.create_task(
                prompt,
                user_id,
                chat_id,
                idempotency_key,
                planner_agent=planner_agent,
                execution_agent=execution_agent,
                target_worker_id=target_worker_id,
                workdir=workdir,
                target_gateway_id=target_gateway_id,
                target_profile=target_profile,
                planning_mode="auto",
            )
            detail = self._route_detail(
                planner_agent,
                execution_agent,
                target_worker_id,
                target_gateway_id,
                target_profile,
                workdir,
            )
            suffix = f"\n{detail}" if detail else ""
            return f"{self._format_created_task(task)}{suffix}"
        if command == "/status":
            if not argument:
                return "用法：/status <任务ID>"
            task = await self.director.get_task(argument)
            return self._format_task_status(task)
        if command == "/cancel":
            if not argument:
                return "用法：/cancel <任务ID>"
            task = await self.director.cancel_task(argument)
            return f"任务 {task['id']} 当前状态：{task['status']}"
        if command.startswith("/"):
            return "未知命令。\n\n" + HELP_TEXT
        directives = self._parse_directives(text)
        if directives is not None:
            (
                prompt,
                wants_planning,
                worker_mentions,
                execution_agent,
                error,
            ) = directives
            if error:
                return error
            workers = await self.director.list_workers()
            worker_ids = {worker["worker_id"] for worker in workers}
            target_worker_id = worker_mentions[0] if worker_mentions else None
            # ``@worker`` is the one-Worker shorthand.  If an installation
            # really uses "worker" as its ID, keep treating it as a target;
            # otherwise let Coordinator choose the available Worker.
            if (
                target_worker_id
                and target_worker_id.casefold() == "worker"
                and target_worker_id not in worker_ids
            ):
                target_worker_id = None
            if target_worker_id and target_worker_id not in worker_ids:
                return (
                    f"未知 Worker：{target_worker_id}。请用 /agents 查看可用 Worker。"
                )
            planning_mode = "plan" if wants_planning else "direct"
            task = await self.director.create_task(
                prompt,
                user_id,
                chat_id,
                idempotency_key,
                planner_agent=None,
                execution_agent=execution_agent,
                target_worker_id=target_worker_id,
                target_gateway_id=None,
                target_profile=None,
                planning_mode=planning_mode,
            )
            detail = self._route_detail(
                None,
                execution_agent,
                target_worker_id,
            )
            suffix = f"\n{detail}" if detail else ""
            return f"{self._format_created_task(task)}{suffix}"
        task = await self.director.create_task(
            text,
            user_id,
            chat_id,
            idempotency_key,
            planner_agent=None,
            execution_agent=None,
            target_worker_id=None,
            target_gateway_id=None,
            target_profile=None,
            planning_mode="auto",
        )
        return self._format_created_task(task)

    @staticmethod
    def _parse_directives(
        text: str,
    ) -> Optional[tuple[str, bool, List[str], Optional[str], Optional[str]]]:
        try:
            tokens = shlex.split(text)
        except ValueError:
            if text.lstrip().startswith("@"):
                return "", False, [], None, "参数错误。指令中的引号不完整。"
            return None
        if not tokens or not tokens[0].startswith("@"):
            return None
        index = 0
        coordinator_count = 0
        worker_mentions: List[str] = []
        while index < len(tokens) and tokens[index].startswith("@"):
            name = tokens[index][1:]
            if not name:
                return "", False, [], None, "@ 后必须指定 Coordinator 或 Worker ID。"
            if name.casefold() == "coordinator":
                coordinator_count += 1
            else:
                worker_mentions.append(name)
            index += 1
        if coordinator_count > 1:
            return "", False, [], None, "@Coordinator 只能指定一次。"
        if len(worker_mentions) > 1:
            if len(set(worker_mentions)) == 1:
                return "", False, [], None, "同一个 Worker 只能指定一次。"
            return (
                "",
                False,
                [],
                None,
                "当前版本一次只能指定一个 Worker；多 Worker 工作流暂未启用。",
            )
        execution_agent: Optional[str] = None
        while index < len(tokens) and (
            tokens[index].startswith("--") or tokens[index] in ("-codex", "-cc")
        ):
            option = tokens[index]
            if option in ("-codex", "-cc"):
                if execution_agent is not None:
                    return "", False, [], None, "参数错误。执行 Agent 只能指定一次。"
                execution_agent = "codex" if option == "-codex" else "claude-code"
                index += 1
                continue
            if option not in ("--agent", "--executor") or index + 1 >= len(tokens):
                return (
                    "",
                    False,
                    [],
                    None,
                    "参数错误。只支持 --executor <agent>（--agent 为兼容别名）。",
                )
            value = tokens[index + 1]
            if not value or value.startswith("--") or execution_agent is not None:
                return "", False, [], None, "参数错误。执行 Agent 只能指定一次。"
            execution_agent = canonical_agent_name(value)
            index += 2
        if index < len(tokens) and tokens[index].startswith("-"):
            return (
                "",
                False,
                [],
                None,
                "参数错误。只支持 -codex、-cc 或 --executor <agent>。",
            )
        prompt = " ".join(tokens[index:]).strip()
        if not prompt:
            return "", False, [], None, "请在 @ 指令后填写任务描述。"
        return (
            prompt,
            coordinator_count == 1,
            worker_mentions[:1],
            execution_agent,
            None,
        )

    @classmethod
    def _format_created_task(cls, task: Dict[str, Any]) -> str:
        mode = task.get("planning_mode", "auto")
        mode_text = "先规划后执行" if mode == "plan" else "直接执行"
        return (
            f"任务已创建：{task['id']}\n模式：{mode_text}\n"
            f"状态：{task['status']}\n阶段：{task.get('current_phase') or 'created'}"
        )

    @classmethod
    def _format_task_status(cls, task: Dict[str, Any]) -> str:
        detail = (
            task.get("result")
            or task.get("error")
            or task.get("routing_diagnostic")
            or "暂无结果"
        )
        route = cls._route_detail(
            task.get("planner_agent"),
            task.get("resolved_execution_agent") or task.get("execution_agent"),
            task.get("resolved_worker_id") or task.get("target_worker_id"),
            task.get("resolved_gateway_id") or task.get("target_gateway_id"),
            task.get("resolved_profile") or task.get("target_profile"),
        )
        lines = [
            f"任务：{task['id']}",
            f"模式：{task.get('planning_mode', 'auto')}",
            f"状态：{task['status']}",
            f"当前阶段：{task.get('current_phase') or 'created'}",
            *cls._stage_lines(task),
        ]
        if route:
            lines.append(route)
        if task.get("attempt_count"):
            lines.append(
                f"执行尝试：{task['attempt_count']}/{task.get('max_attempts', '?')}"
            )
        events = task.get("recent_events") or []
        if events:
            lines.append("最近事件：")
            for event in events[-5:]:
                message = event.get("message") or event.get("event_type")
                lines.append(f"- {event.get('phase') or 'event'}：{message}")
        lines.append(f"结果：{detail}")
        return "\n".join(lines)

    @staticmethod
    def _stage_lines(task: Dict[str, Any]) -> List[str]:
        status = task["status"]
        planned = task.get("planning_mode") == "plan"
        stages = (
            ["规划", "排队", "执行", "结果"] if planned else ["排队", "执行", "结果"]
        )
        if status in ("planning_pending", "planning"):
            active = 0
        elif status == "pending":
            active = 1 if planned else 0
        elif status in ("claimed", "running", "reconciling", "cancel_requested"):
            active = 2 if planned else 1
        else:
            active = len(stages) - 1
        lines = ["进度："]
        for index, stage in enumerate(stages):
            if index < active:
                marker = "✓"
            elif index == active:
                marker = "✓" if status == "succeeded" and stage == "结果" else "▶"
            else:
                marker = "○"
            lines.append(f"{marker} {stage}")
        return lines

    @staticmethod
    def _parse_new_arguments(
        argument: str,
    ) -> Optional[
        tuple[
            str,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
        ]
    ]:
        """Parse the optional routing flags without creating a task on errors."""
        try:
            tokens = shlex.split(argument)
        except ValueError:
            return None
        planner_agent: Optional[str] = None
        execution_agent: Optional[str] = None
        target_worker_id: Optional[str] = None
        workdir: Optional[str] = None
        target_gateway_id: Optional[str] = None
        target_profile: Optional[str] = None
        index = 0
        while index < len(tokens) and tokens[index].startswith("--"):
            option = tokens[index]
            if option not in (
                "--planner",
                "--agent",
                "--executor",
                "--worker",
                "--workdir",
                "--gateway",
                "--profile",
            ) or index + 1 >= len(tokens):
                return None
            value = tokens[index + 1]
            if not value or value.startswith("--"):
                return None
            if option == "--planner":
                if planner_agent is not None or value != C2C_PLANNER_AGENT:
                    return None
                planner_agent = value
            elif option in ("--agent", "--executor"):
                if execution_agent is not None:
                    return None
                execution_agent = canonical_agent_name(value)
            elif option == "--worker":
                if target_worker_id is not None:
                    return None
                target_worker_id = value
            elif option == "--workdir":
                if workdir is not None:
                    return None
                workdir = value
            elif option == "--gateway":
                if target_gateway_id is not None:
                    return None
                target_gateway_id = value
            else:
                if target_profile is not None:
                    return None
                target_profile = value
            index += 2
        if index >= len(tokens):
            return None
        if (target_gateway_id is None) != (target_profile is None):
            return None
        return (
            " ".join(tokens[index:]),
            planner_agent,
            execution_agent,
            target_worker_id,
            workdir,
            target_gateway_id,
            target_profile,
        )

    @staticmethod
    def _route_detail(
        planner_agent: Optional[str],
        execution_agent: Optional[str],
        worker_id: Optional[str] = None,
        gateway_id: Optional[str] = None,
        profile: Optional[str] = None,
        workdir: Optional[str] = None,
    ) -> str:
        values = []
        if planner_agent:
            values.append(f"规划 Agent：{planner_agent}")
        if execution_agent:
            values.append(f"执行 Agent：{execution_agent}")
        if worker_id:
            values.append(f"Worker：{worker_id}")
        if gateway_id and profile:
            values.append(f"Gateway/Profile：{gateway_id}/{profile}")
        if workdir:
            values.append(f"目录：{workdir}")
        return " | ".join(values)

    def load_offset(self) -> Optional[int]:
        path = Path(self.settings.offset_path)
        if not path.is_file():
            return None
        try:
            return int(path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            LOGGER.exception("could not read Telegram offset file")
            return None

    def save_offset(self, offset: int) -> None:
        path = Path(self.settings.offset_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_suffix(path.suffix + ".tmp")
        temporary_path.write_text(str(offset), encoding="utf-8")
        temporary_path.replace(path)

    async def updates_loop(self) -> None:
        offset = self.load_offset()
        while not self.stopping.is_set():
            try:
                updates = await self.telegram.get_updates(
                    offset, self.settings.poll_timeout_seconds
                )
                for update in updates:
                    await self.handle_update(update)
                    offset = int(update["update_id"]) + 1
                    self.save_offset(offset)
            except Exception:
                LOGGER.exception("Telegram polling failed")
                await asyncio.sleep(3)

    async def notifications_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                notifications = await self.director.notifications()
                for notification in notifications:
                    if notification["channel"] != "telegram":
                        continue
                    payload = notification["payload"]
                    detail = payload.get("result") or payload.get("error") or "无输出"
                    text = (
                        f"任务完成：{payload['task_id']}\n"
                        f"状态：{payload['status']}\n结果：{detail}"
                    )
                    await self.telegram.send_message(
                        int(notification["destination"]), text
                    )
                    await self.director.acknowledge(notification["id"])
            except Exception:
                LOGGER.exception("notification delivery failed")
            try:
                await asyncio.wait_for(
                    self.stopping.wait(),
                    timeout=self.settings.notification_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass

    async def run(self) -> None:
        await asyncio.gather(self.updates_loop(), self.notifications_loop())


async def run_bot(settings: TelegramSettings) -> None:
    if not settings.bot_token:
        raise ValueError("HERMES_TELEGRAM_BOT_TOKEN is required for Telegram mode")
    if not settings.allowed_user_ids:
        raise ValueError("HERMES_TELEGRAM_ALLOWED_USER_IDS must not be empty")
    if not settings.director_api_key:
        raise ValueError("HERMES_DIRECTOR_API_KEY is required")
    director = DirectorAPI(settings.coordinator_url, settings.director_api_key)
    telegram = TelegramGateway(settings.bot_token)
    bot = DirectorBot(settings, director, telegram)
    try:
        await bot.run()
    finally:
        await director.close()
        await telegram.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Hermes Director Telegram Bot")
    parser.add_argument("--log-level", default="INFO")
    arguments = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, arguments.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_bot(TelegramSettings.from_env()))


if __name__ == "__main__":
    main()
