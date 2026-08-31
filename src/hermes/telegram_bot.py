from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .config import TelegramSettings

LOGGER = logging.getLogger("hermes.telegram")
HELP_TEXT = """Hermes Director 命令：
/agents - 查看 Worker 在线状态
/new <任务> - 创建任务
/status <任务ID> - 查询任务
/cancel <任务ID> - 取消任务
/help - 显示帮助

直接发送普通文本也会创建任务。"""


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

    async def create_task(
        self,
        prompt: str,
        user_id: int,
        chat_id: int,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            "/api/v1/tasks",
            {
                "prompt": prompt,
                "creator_user_id": str(user_id),
                "telegram_chat_id": str(chat_id),
                "idempotency_key": idempotency_key,
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
        command, separator, argument = text.partition(" ")
        command = command.split("@", 1)[0].lower()
        argument = argument.strip() if separator else ""
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
        if command == "/new":
            if not argument:
                return "用法：/new <任务描述>"
            task = await self.director.create_task(
                argument, user_id, chat_id, idempotency_key
            )
            return f"任务已创建：{task['id']}\n状态：{task['status']}"
        if command == "/status":
            if not argument:
                return "用法：/status <任务ID>"
            task = await self.director.get_task(argument)
            detail = task.get("result") or task.get("error") or "暂无结果"
            return f"任务：{task['id']}\n状态：{task['status']}\n详情：{detail}"
        if command == "/cancel":
            if not argument:
                return "用法：/cancel <任务ID>"
            task = await self.director.cancel_task(argument)
            return f"任务 {task['id']} 当前状态：{task['status']}"
        if command.startswith("/"):
            return "未知命令。\n\n" + HELP_TEXT
        task = await self.director.create_task(text, user_id, chat_id, idempotency_key)
        return f"任务已创建：{task['id']}\n状态：{task['status']}"

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
