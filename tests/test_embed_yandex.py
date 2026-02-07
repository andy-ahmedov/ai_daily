from __future__ import annotations

from types import SimpleNamespace

from aidigest.nlp import embed as embed_module


class _DummyItem:
    def __init__(self, index: int, embedding: list[float]) -> None:
        self.index = index
        self.embedding = embedding


class _DummyResponse:
    def __init__(self, data: list[_DummyItem]) -> None:
        self.data = data


class _DummyEmbeddings:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    def create(self, *, model: str, input: str, encoding_format: str) -> _DummyResponse:
        assert model == "emb://dummy/model"
        assert encoding_format == "float"
        assert isinstance(input, str)
        self.inputs.append(input)
        return _DummyResponse([_DummyItem(0, [0.1, 0.2, 0.3])])


class _DummyClient:
    def __init__(self) -> None:
        self.embeddings = _DummyEmbeddings()


def test_embed_texts_splits_batch_into_single_requests(monkeypatch) -> None:
    dummy_client = _DummyClient()
    monkeypatch.setattr(embed_module, "_DEFAULT_CLIENT", dummy_client)
    monkeypatch.setattr(
        embed_module,
        "get_settings",
        lambda: SimpleNamespace(
            yandex_embed_model_uri="emb://dummy/model",
            embed_dim=3,
            yandex_api_key="dummy",
            yandex_folder_id="dummy",
        ),
    )

    vectors = embed_module.embed_texts(["hello", "world"])

    assert dummy_client.embeddings.inputs == ["hello", "world"]
    assert vectors == [[0.1, 0.2, 0.3], [0.1, 0.2, 0.3]]
