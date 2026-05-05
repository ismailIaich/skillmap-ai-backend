from __future__ import annotations

from typing import Any

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.skill import Skill
from app.services.embedding.encoder import encode_text
from app.services.embedding.similarity import cosine_similarity


def _is_usable_embedding(value: object) -> bool:
    if value is None:
        return False
    if not isinstance(value, (list, tuple)):
        return False
    if len(value) == 0:
        return False
    try:
        for x in value:
            float(x)
    except (TypeError, ValueError):
        return False
    return True


def extract_skills_from_text(
    db: Session,
    text: str,
    threshold: float = 0.7,
) -> list[dict[str, Any]]:
    """
    Map free-text input to skills using stored embeddings and cosine similarity.

    Returns all skills at or above ``threshold``, sorted by similarity (desc).
    Does not modify skills or embeddings.
    """
    if not text or not text.strip():
        return []

    logger.info(
        "skill extract | input length={length}",
        length=len(text),
    )

    try:
        query_vector = encode_text(text)
    except Exception:
        logger.warning(
            "skill extract | query embedding failed | input length={length}",
            length=len(text),
        )
        return []

    if not query_vector:
        logger.warning(
            "skill extract | empty query vector | input length={length}",
            length=len(text),
        )
        return []

    skills = list(db.scalars(select(Skill)).all())
    scanned = len(skills)
    logger.debug("skill extract | skills scanned={n}", n=scanned)

    scored: list[tuple[float, Skill]] = []

    for skill in skills:
        if skill.embedding is None:
            continue

        emb = skill.embedding
        if not _is_usable_embedding(emb):
            logger.warning(
                "skill extract | skip malformed embedding | skill_id={skill_id}",
                skill_id=skill.id,
            )
            continue

        try:
            score = cosine_similarity(query_vector, emb)
        except Exception:
            logger.warning(
                "skill extract | skip similarity error | skill_id={skill_id}",
                skill_id=skill.id,
            )
            continue

        if score >= threshold:
            scored.append((score, skill))

    scored.sort(key=lambda item: item[0], reverse=True)

    out: list[dict[str, Any]] = [
        {
            "skill_id": str(skill.id),
            "name": skill.name,
            "category": skill.category,
            "similarity": float(sim),
        }
        for sim, skill in scored
    ]

    logger.info(
        "skill extract | returned={n}",
        n=len(out),
    )

    return out
