from __future__ import annotations

from aidigest.digest.format import MAX_MESSAGE_LEN, _split_block


def test_split_block_respects_limit() -> None:
    source = "x" * (MAX_MESSAGE_LEN * 2 + 123)
    chunks = _split_block(source, MAX_MESSAGE_LEN)

    assert len(chunks) == 3
    assert all(len(chunk) <= MAX_MESSAGE_LEN for chunk in chunks)
    assert "".join(chunks) == source
