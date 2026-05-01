from __future__ import annotations

import uuid

from loguru import logger
from sqlalchemy.orm import Session

from app.models.skill import Skill
from app.services.embedding.encoder import encode_text


def embedding_source_text(name: str, category: str) -> str:
    """Text used for skill embedding (name + category)."""
    return f"{name.strip()} | {category.strip()}"


def create_skill_with_embedding(db: Session, name: str, category: str) -> Skill:
    """
    Generate embedding, persist a new Skill, and commit.

    Pass a DB session from FastAPI `Depends(get_db)` or from `SessionLocal()` in scripts.
    """
    source = embedding_source_text(name, category)
    vector = encode_text(source)

    if not vector:
        logger.warning(
            "skill embedding | create | empty vector after encode | name={name}",
            name=name,
        )
    else:
        logger.info(
            "skill embedding | create | generated | dim={dim} | name={name}",
            dim=len(vector),
            name=name,
        )

    skill = Skill(name=name, category=category, embedding=vector if vector else None)
    db.add(skill)
    db.commit()
    db.refresh(skill)
    return skill


def get_skill_embedding(db: Session, skill_id: uuid.UUID) -> list[float] | None:
    """Return stored embedding for a skill, or None if missing skill or NULL embedding."""
    skill = db.get(Skill, skill_id)
    if skill is None:
        return None
    return skill.embedding
