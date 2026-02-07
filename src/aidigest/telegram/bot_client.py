from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

_DEFAULT_WAIT = wait_random_exponential(multiplier=0.5, min=0.5, max=10)


@dataclass(slots=True)
class TelegramAPIError(RuntimeError):
    status_code: int | None
    description: str
    retry_after: float | None = None
    retryable: bool = False

    def __str__(self) -> str:
        if self.status_code is None:
            return self.description
        return f"[{self.status_code}] {self.description}"


def _extract_retry_after(payload: dict[str, Any] | None) -> float | None:
    if not isinstance(payload, dict):
        return None

    params = payload.get("parameters")
    if isinstance(params, dict):
        value = params.get("retry_after")
        if isinstance(value, (int, float)) and value > 0:
            return float(value)

    description = payload.get("description")
    if isinstance(description, str):
        lowered = description.lower()
        marker = "retry after"
        if marker in lowered:
            suffix = lowered.split(marker, 1)[1].strip()
            token = suffix.split(" ", 1)[0]
            try:
                parsed = float(token)
                if parsed > 0:
                    return parsed
            except ValueError:
                return None
    return None


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)):
        return True
    if isinstance(exc, TelegramAPIError):
        return exc.retryable
    return False


def _wait_strategy(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception()
    if isinstance(exc, TelegramAPIError) and exc.retry_after and exc.retry_after > 0:
        return float(exc.retry_after) + random.uniform(0.05, 0.35)
    return float(_DEFAULT_WAIT(retry_state))


def _before_sleep(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception()
    logger.warning(
        "Telegram publish retry: attempt={} reason={}",
        retry_state.attempt_number,
        exc if exc is not None else "unknown",
    )


class DigestPublisher:
    def __init__(self, bot_token: str) -> None:
        token = (bot_token or "").strip()
        if not token:
            raise RuntimeError("BOT_TOKEN is not set")
        self._bot_token = token
        self._client = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> DigestPublisher:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def _method_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self._bot_token}/{method}"

    def send_html_messages(self, chat_id: int, messages: list[str]) -> list[int]:
        message_ids: list[int] = []
        for message in messages:
            message_id = self._send_html_message(chat_id=chat_id, text=message)
            message_ids.append(message_id)
        return message_ids

    @retry(
        retry=retry_if_exception(_is_retryable_exception),
        wait=_wait_strategy,
        stop=stop_after_attempt(6),
        reraise=True,
        before_sleep=_before_sleep,
    )
    def _send_html_message(self, *, chat_id: int, text: str) -> int:
        response = self._client.post(
            self._method_url("sendMessage"),
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        )

        payload: dict[str, Any] | None = None
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if response.status_code >= 500:
            raise TelegramAPIError(
                status_code=response.status_code,
                description=f"telegram server error: {response.text[:500]}",
                retryable=True,
            )

        if response.status_code == 429:
            raise TelegramAPIError(
                status_code=response.status_code,
                description="telegram rate limited request",
                retry_after=_extract_retry_after(payload),
                retryable=True,
            )

        if not response.is_success:
            raise TelegramAPIError(
                status_code=response.status_code,
                description=f"telegram request failed: {response.text[:500]}",
                retryable=False,
            )

        if not isinstance(payload, dict):
            raise TelegramAPIError(
                status_code=response.status_code,
                description="telegram returned non-json response",
                retryable=False,
            )
        if payload.get("ok") is not True:
            error_code = payload.get("error_code")
            description = str(payload.get("description", "telegram api error"))
            retry_after = _extract_retry_after(payload)
            retryable = bool(
                error_code == 429 or (isinstance(error_code, int) and error_code >= 500)
            )
            raise TelegramAPIError(
                status_code=int(error_code)
                if isinstance(error_code, int)
                else response.status_code,
                description=description,
                retry_after=retry_after,
                retryable=retryable,
            )

        result = payload.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("message_id"), int):
            raise TelegramAPIError(
                status_code=response.status_code,
                description="telegram response missing message_id",
                retryable=False,
            )
        return int(result["message_id"])
