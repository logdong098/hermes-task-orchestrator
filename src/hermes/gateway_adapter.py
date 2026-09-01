"""Small async client for a Hermes Gateway's profile-scoped Runs API."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx


class GatewayAdapter:
    def __init__(
        self,
        base_url: str,
        token: str = "",
        profile_keys: Optional[Dict[str, str]] = None,
        default_profile: str = "",
        configured_profiles: Optional[List[str]] = None,
        timeout: float = 30.0,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.profile_keys = profile_keys or {}
        self.default_profile = default_profile
        self.configured_profiles = configured_profiles or []
        self.client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    def _headers(
        self, idempotency_key: Optional[str] = None, profile: Optional[str] = None
    ) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if profile is None:
            token = self.token
        elif profile in self.profile_keys:
            token = self.profile_keys[profile]
        elif profile == self.default_profile:
            token = self.token
        else:
            raise ValueError(f"no API key configured for Gateway profile: {profile}")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        response = await self.client.request(
            method,
            f"{self.base_url}{path}",
            json=payload,
            headers=self._headers(idempotency_key, profile),
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    async def discover_profiles(self) -> List[str]:
        data = await self._request("GET", "/api/profiles")
        profiles = data.get("profiles", data) if isinstance(data, dict) else data
        if not isinstance(profiles, list):
            return []
        return [
            profile
            for item in profiles
            if isinstance(item, str) or isinstance(item, dict)
            for profile in [
                item
                if isinstance(item, str)
                else item.get("name", item.get("profile", ""))
            ]
            if profile
        ]

    async def capabilities(self, profile: str) -> Dict[str, Any]:
        return await self._request(
            "GET", f"/p/{quote(profile, safe='')}/v1/capabilities", profile=profile
        )

    async def start_run(
        self,
        profile: str,
        prompt: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        # The Runs API accepts the user input under ``input``. Executor
        # selection is resolved by the Coordinator route in M1; arbitrary
        # agent fields are intentionally not sent to the Gateway API.
        payload: Dict[str, Any] = {"input": prompt}
        return await self._request(
            "POST",
            f"/p/{quote(profile, safe='')}/v1/runs",
            payload,
            idempotency_key,
            profile,
        )

    async def get_run(self, profile: str, run_id: str) -> Dict[str, Any]:
        return await self._request(
            "GET",
            f"/p/{quote(profile, safe='')}/v1/runs/{quote(run_id, safe='')}",
            profile=profile,
        )

    async def stop_run(self, profile: str, run_id: str) -> Dict[str, Any]:
        return await self._request(
            "POST",
            f"/p/{quote(profile, safe='')}/v1/runs/{quote(run_id, safe='')}/stop",
            profile=profile,
        )
