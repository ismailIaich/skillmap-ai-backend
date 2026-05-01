from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.skill import Skill
from app.services.skill_embedding_service import embedding_source_text
from app.services.embedding.encoder import encode_text


def backfill_embeddings(db: Session) -> None:
    """Set `embedding` on skills where it is NULL. Skips rows that already have an embedding."""
    skills = list(db.scalars(select(Skill)).all())

    for skill in skills:
        if skill.embedding is not None:
            logger.info(
                "skill embedding | backfill | skipped | skill_id={skill_id} (already set)",
                skill_id=skill.id,
            )
            continue

        source = embedding_source_text(skill.name, skill.category)
        vector = encode_text(source)

        if not vector:
            logger.warning(
                "skill embedding | backfill | skipped | skill_id={skill_id} (empty vector)",
                skill_id=skill.id,
            )
            continue

        skill.embedding = vector
        db.commit()
        logger.info(
            "skill embedding | backfill | updated | skill_id={skill_id} | dim={dim}",
            skill_id=skill.id,
            dim=len(vector),
        )


def main() -> None:
    db = SessionLocal()
    try:
        backfill_embeddings(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
