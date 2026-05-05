from __future__ import annotations

"""
Compatibility shim.

The codebase historically used `SkillOccupation` for the join table between skills and occupations.
Some callers expect `OccupationSkill` instead. This module provides a stable import path without
changing the underlying table or ORM mapping.
"""

from app.models.skill_occupation import SkillOccupation as OccupationSkill

__all__ = ["OccupationSkill"]

