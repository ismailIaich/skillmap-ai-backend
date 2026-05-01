from __future__ import annotations

from functools import lru_cache

from loguru import logger
from sentence_transformers import SentenceTransformer


MODEL_NAME = "all-MiniLM-L6-v2"


@lru_cache(maxsize=1)
def get_embedding_model() -> SentenceTransformer:
    logger.info("embedding model load | name={name}", name=MODEL_NAME)
    return SentenceTransformer(MODEL_NAME)

