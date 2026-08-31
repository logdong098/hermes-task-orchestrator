from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from typing import Any, Dict, Optional


def compact_json(data: Optional[Dict[str, Any]]) -> bytes:
    if data is None:
        return b""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def body_digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def signing_message(
    timestamp: str, nonce: str, method: str, path: str, body: bytes
) -> bytes:
    return (
        f"{timestamp}\n{nonce}\n{method.upper()}\n{path}\n{body_digest(body)}"
    ).encode("utf-8")


def sign_request(
    secret: str,
    method: str,
    path: str,
    body: bytes = b"",
    timestamp: Optional[int] = None,
    nonce: Optional[str] = None,
) -> Dict[str, str]:
    signed_at = str(timestamp if timestamp is not None else int(time.time()))
    request_nonce = nonce or secrets.token_urlsafe(18)
    signature = hmac.new(
        secret.encode("utf-8"),
        signing_message(signed_at, request_nonce, method, path, body),
        hashlib.sha256,
    ).hexdigest()
    return {
        "X-Hermes-Timestamp": signed_at,
        "X-Hermes-Nonce": request_nonce,
        "X-Hermes-Signature": signature,
    }


def verify_signature(
    secret: str,
    method: str,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
    signature: str,
    max_clock_skew_seconds: int,
    now: Optional[int] = None,
) -> bool:
    if not secret or not timestamp or not nonce or not signature:
        return False
    try:
        signed_at = int(timestamp)
    except ValueError:
        return False
    current = now if now is not None else int(time.time())
    if abs(current - signed_at) > max_clock_skew_seconds:
        return False
    expected = sign_request(secret, method, path, body, signed_at, nonce)[
        "X-Hermes-Signature"
    ]
    return hmac.compare_digest(expected, signature)
