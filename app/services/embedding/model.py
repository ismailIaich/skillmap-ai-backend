from __future__ import annotations

from loguru import logger
from sentence_transformers import SentenceTransformer


MODEL_NAME = "all-MiniLM-L6-v2"

_model: SentenceTransformer | None = None


def get_embedding_model() -> SentenceTransformer:
    global _model
    if _model is None:
        logger.info("embedding model load | name={name}", name=MODEL_NAME)
        _model = SentenceTransformer(MODEL_NAME)
    return _model
