"""Shared O*NET loader datatypes (used by scripts and persistence)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OnetSkill:
    skill_id: str
    name: str
    description: str


@dataclass(frozen=True)
class OnetOccupation:
    occupation_id: str
    title: str


@dataclass(frozen=True)
class OnetOccupationSkill:
    occupation_id: str
    skill_id: str
    importance: float
