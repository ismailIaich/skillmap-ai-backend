from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.skill_occupation import SkillOccupation
    from app.models.skill import Skill
    from app.models.task import Task


class Occupation(Base):
    __tablename__ = "occupations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    isco_code: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    sector: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    skill_occupations: Mapped[list["SkillOccupation"]] = relationship(
        back_populates="occupation",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    skills: Mapped[list["Skill"]] = relationship(
        secondary="skill_occupations",
        back_populates="occupations",
        viewonly=True,
    )

    tasks: Mapped[list["Task"]] = relationship(
        back_populates="occupation",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

