from __future__ import annotations

import json
import re
from typing import Any

from loguru import logger
from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_random_exponential

from aidigest.config import Settings


_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*\})\s*```", re.DOTALL | re.IGNORECASE)


class InvalidJSONResponseError(RuntimeError):
    pass


_DEFAULT_CLIENT: OpenAI | None = None


def make_client(settings: Settings) -> OpenAI:
    if not settings.yandex_api_key:
        raise RuntimeError("YANDEX_API_KEY is not set")
    if not settings.yandex_folder_id:
        raise RuntimeError("YANDEX_FOLDER_ID is not set")

    client = OpenAI(
        api_key=settings.yandex_api_key,
        base_url="https://ai.api.cloud.yandex.net/v1",
        project=settings.yandex_folder_id,
        max_retries=0,
    )
    global _DEFAULT_CLIENT
    _DEFAULT_CLIENT = client
    return client


def _normalize_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                parts.append(str(item.get("text", "")))
                continue
            parts.append(str(getattr(item, "text", "")))
        return "".join(parts)
    return str(content or "")


def _parse_json_payload(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if not text:
        raise InvalidJSONResponseError("empty response content")

    block_match = _JSON_BLOCK_RE.search(text)
    if block_match:
        text = block_match.group(1).strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidate = text[start : end + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError as exc:
            raise InvalidJSONResponseError("response is not valid JSON object") from exc

    raise InvalidJSONResponseError("response is not valid JSON object")


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, (APIConnectionError, APITimeoutError, InvalidJSONResponseError)):
        return True
    if isinstance(exc, APIStatusError):
        status_code = getattr(exc, "status_code", None)
        return status_code == 429 or (isinstance(status_code, int) and status_code >= 500)
    return False


def _before_sleep(retry_state: RetryCallState) -> None:
    post_id = retry_state.kwargs.get("post_id")
    exc = retry_state.outcome.exception()
    logger.warning(
        "LLM retry: post_id={} attempt={} reason={}",
        post_id,
        retry_state.attempt_number,
        exc.__class__.__name__ if exc is not None else "unknown",
    )


@retry(
    retry=retry_if_exception(_is_retryable_exception),
    wait=wait_random_exponential(multiplier=0.5, min=0.5, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
    before_sleep=_before_sleep,
)
def chat_json(
    model_uri: str,
    messages: list[dict[str, str]],
    *,
    client: OpenAI | None = None,
    post_id: int | None = None,
) -> dict[str, Any]:
    resolved_client = client or _DEFAULT_CLIENT
    if resolved_client is None:
        raise RuntimeError("LLM client is not initialized")

    response = resolved_client.chat.completions.create(
        model=model_uri,
        messages=messages,
        temperature=0,
        response_format={"type": "json_object"},
    )
    if not response.choices:
        raise InvalidJSONResponseError("empty choices")

    content = _normalize_content(response.choices[0].message.content)
    return _parse_json_payload(content)
