from __future__ import annotations

from typing import TypedDict

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.skill import Skill
from app.services.embedding.encoder import encode_text
from app.services.embedding.similarity import cosine_similarity


class SkillMatch(TypedDict):
    skill_id: str
    name: str
    category: str
    similarity: float


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


def match_skills(
    db: Session,
    query: str,
    top_k: int = 5,
    threshold: float = 0.5,
) -> list[SkillMatch]:
    """
    Rank stored skill embeddings against an encoded query (query embedding computed once).

    Does not recompute or modify skill embeddings.
    """
    if not query or not query.strip():
        return []

    if top_k < 1:
        return []

    trimmed = query.strip()
    logger.info("semantic match | query={q}", q=trimmed)

    query_embedding = encode_text(trimmed)
    if not query_embedding:
        logger.info(
            "semantic match | scanned={scanned} | returned={returned}",
            scanned=0,
            returned=0,
        )
        return []

    skills = list(db.scalars(select(Skill)).all())
    total_scanned = len(skills)
    logger.debug("semantic match | total skills fetched={n}", n=total_scanned)

    scored: list[tuple[float, Skill]] = []

    for skill in skills:
        if skill.embedding is None:
            continue

        emb = skill.embedding
        if not _is_usable_embedding(emb):
            logger.warning(
                "semantic match | skip malformed embedding | skill_id={skill_id}",
                skill_id=skill.id,
            )
            continue

        try:
            score = cosine_similarity(query_embedding, emb)
        except Exception:
            logger.warning(
                "semantic match | skip similarity error | skill_id={skill_id}",
                skill_id=skill.id,
            )
            continue

        if score >= threshold:
            scored.append((score, skill))

    scored.sort(key=lambda item: item[0], reverse=True)
    top = scored[:top_k]

    out: list[SkillMatch] = [
        {
            "skill_id": str(skill.id),
            "name": skill.name,
            "category": skill.category,
            "similarity": float(sim),
        }
        for sim, skill in top
    ]

    logger.info(
        "semantic match | scanned={scanned} | returned={returned}",
        scanned=total_scanned,
        returned=len(out),
    )

    return out
