from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.skill import Skill
from app.services.skill_embedding_service import create_skill_with_embedding


# (name, category) — unique names required (Skill.name is unique).
SKILL_ROWS: list[tuple[str, str]] = [
    # AI / Data (minimum set + expansion)
    ("Machine Learning", "AI / Data"),
    ("Deep Learning", "AI / Data"),
    ("Data Science", "AI / Data"),
    ("NLP", "AI / Data"),
    ("Computer Vision", "AI / Data"),
    ("PyTorch", "AI / Data"),
    ("TensorFlow", "AI / Data"),
    ("scikit-learn", "AI / Data"),
    ("Large Language Models", "AI / Data"),
    ("Prompt Engineering", "AI / Data"),
    ("MLOps", "AI / Data"),
    ("Model Evaluation", "AI / Data"),
    ("Feature Engineering", "AI / Data"),
    ("Pandas", "AI / Data"),
    ("NumPy", "AI / Data"),
    ("Data Visualization", "AI / Data"),
    ("Statistics", "AI / Data"),
    ("Experiment Tracking", "AI / Data"),
    ("Data Engineering", "AI / Data"),
    ("ETL", "AI / Data"),
    ("Apache Spark", "AI / Data"),
    ("Vector Databases", "AI / Data"),
    ("Information Retrieval", "AI / Data"),
    ("Time Series Analysis", "AI / Data"),
    # Backend
    ("Python", "Backend"),
    ("FastAPI", "Backend"),
    ("REST APIs", "Backend"),
    ("SQL", "Backend"),
    ("PostgreSQL", "Backend"),
    ("Django", "Backend"),
    ("Flask", "Backend"),
    ("GraphQL", "Backend"),
    ("Redis", "Backend"),
    ("MongoDB", "Backend"),
    ("Microservices", "Backend"),
    ("API Design", "Backend"),
    ("Authentication & Authorization", "Backend"),
    ("ORM", "Backend"),
    ("Database Design", "Backend"),
    ("Caching Strategies", "Backend"),
    ("Message Queues", "Backend"),
    ("WebSockets", "Backend"),
    ("OpenAPI", "Backend"),
    ("Testing (Backend)", "Backend"),
    # Frontend
    ("JavaScript", "Frontend"),
    ("TypeScript", "Frontend"),
    ("React", "Frontend"),
    ("HTML", "Frontend"),
    ("CSS", "Frontend"),
    ("Vue.js", "Frontend"),
    ("Next.js", "Frontend"),
    ("Responsive Design", "Frontend"),
    ("Accessibility (a11y)", "Frontend"),
    ("State Management", "Frontend"),
    ("Frontend Testing", "Frontend"),
    ("Webpack / Bundlers", "Frontend"),
    ("Web Performance", "Frontend"),
    ("Browser APIs", "Frontend"),
    # DevOps
    ("Docker", "DevOps"),
    ("Kubernetes", "DevOps"),
    ("CI/CD", "DevOps"),
    ("Linux", "DevOps"),
    ("Terraform", "DevOps"),
    ("AWS", "DevOps"),
    ("GitHub Actions", "DevOps"),
    ("Monitoring & Observability", "DevOps"),
    ("Infrastructure as Code", "DevOps"),
    ("Shell Scripting", "DevOps"),
    ("Networking Basics", "DevOps"),
    ("Secrets Management", "DevOps"),
    ("Logging & Tracing", "DevOps"),
    # Business / Soft Skills
    ("Project Management", "Business / Soft Skills"),
    ("Communication", "Business / Soft Skills"),
    ("Leadership", "Business / Soft Skills"),
    ("Problem Solving", "Business / Soft Skills"),
    ("Stakeholder Management", "Business / Soft Skills"),
    ("Agile", "Business / Soft Skills"),
    ("Scrum", "Business / Soft Skills"),
    ("Requirements Gathering", "Business / Soft Skills"),
    ("Technical Writing", "Business / Soft Skills"),
    ("Presentation Skills", "Business / Soft Skills"),
    ("Collaboration", "Business / Soft Skills"),
    ("Time Management", "Business / Soft Skills"),
    ("Critical Thinking", "Business / Soft Skills"),
]


def seed_skills(db: Session) -> None:
    """Insert skills from SKILL_ROWS when missing (by exact name). Embeddings via create_skill_with_embedding."""
    added = 0
    skipped = 0

    for name, category in SKILL_ROWS:
        existing = db.scalars(select(Skill).where(Skill.name == name)).first()
        if existing is not None:
            skipped += 1
            continue

        create_skill_with_embedding(db, name, category)
        added += 1

    logger.info("seed skills | added={added}", added=added)
    logger.info("seed skills | skipped={skipped} (already exist)", skipped=skipped)


if __name__ == "__main__":
    db = SessionLocal()
    try:
        seed_skills(db)
    finally:
        db.close()
