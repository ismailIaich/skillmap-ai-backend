from __future__ import annotations

from loguru import logger

from app.services.embedding.model import get_embedding_model


def encode_text(text: str) -> list[float]:
    """
    Encode a single text into an embedding vector.

    Returns a Python list[float]. Empty/whitespace input returns [].
    """
    if not text or not text.strip():
        logger.debug("encode_text | empty input")
        return []

    model = get_embedding_model()
    logger.debug("encode_text | chars={n}", n=len(text))
    vec = model.encode(text, normalize_embeddings=False)
    return vec.tolist()


def encode_batch(texts: list[str]) -> list[list[float]]:
    """
    Encode a batch of texts.

    Returns a list[list[float]]. Empty input list returns [].
    Individual empty strings return [] for that entry.
    """
    if not texts:
        logger.debug("encode_batch | empty batch")
        return []

    model = get_embedding_model()

    # Preserve input order; avoid calling the model for empty items.
    idx_and_text: list[tuple[int, str]] = [
        (i, t) for i, t in enumerate(texts) if t and t.strip()
    ]
    out: list[list[float]] = [[] for _ in texts]

    if not idx_and_text:
        logger.debug("encode_batch | all empty items | count={n}", n=len(texts))
        return out

    indices, non_empty = zip(*idx_and_text)
    logger.debug("encode_batch | items={n}", n=len(non_empty))

    vectors = model.encode(list(non_empty), normalize_embeddings=False)
    vectors_list: list[list[float]] = vectors.tolist()

    for i, vec in zip(indices, vectors_list, strict=True):
        out[i] = vec

    return out

