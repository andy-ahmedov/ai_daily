from __future__ import annotations

from math import isfinite

from loguru import logger
from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_random_exponential

from aidigest.config import Settings, get_settings


_DEFAULT_CLIENT: OpenAI | None = None


def make_yandex_client(settings: Settings) -> OpenAI:
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


def validate_embedding(vec: list[float] | tuple[float, ...]) -> list[float]:
    settings = get_settings()
    if len(vec) != settings.embed_dim:
        raise ValueError(
            f"embedding length mismatch: expected {settings.embed_dim}, got {len(vec)}"
        )

    normalized: list[float] = []
    for idx, value in enumerate(vec):
        if not isinstance(value, (int, float)):
            raise TypeError(f"embedding[{idx}] must be float, got {type(value).__name__}")
        float_value = float(value)
        if not isfinite(float_value):
            raise ValueError(f"embedding[{idx}] must be finite")
        normalized.append(float_value)
    return normalized


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, (APIConnectionError, APITimeoutError)):
        return True
    if isinstance(exc, APIStatusError):
        status_code = getattr(exc, "status_code", None)
        return status_code == 429 or (isinstance(status_code, int) and status_code >= 500)
    return False


def _before_sleep(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception()
    texts = retry_state.kwargs.get("texts", [])
    logger.warning(
        "Embedding retry: batch_size={} attempt={} reason={}",
        len(texts),
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
def _embed_with_retry(
    *,
    client: OpenAI,
    model_uri: str,
    texts: list[str],
) -> list[list[float]]:
    response = client.embeddings.create(
        model=model_uri,
        input=texts,
    )
    data = sorted(response.data, key=lambda item: int(getattr(item, "index", 0)))
    vectors: list[list[float]] = []
    for item in data:
        embedding = getattr(item, "embedding", None)
        if embedding is None:
            raise RuntimeError("embedding response item has no embedding")
        vectors.append(validate_embedding(list(embedding)))

    if len(vectors) != len(texts):
        raise RuntimeError(
            f"embedding response size mismatch: expected {len(texts)}, got {len(vectors)}"
        )
    return vectors


def embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []

    settings = get_settings()
    if not settings.yandex_embed_model_uri:
        raise RuntimeError("YANDEX_EMBED_MODEL_URI is not set")

    client = _DEFAULT_CLIENT or make_yandex_client(settings)
    return _embed_with_retry(
        client=client,
        model_uri=settings.yandex_embed_model_uri,
        texts=texts,
    )
