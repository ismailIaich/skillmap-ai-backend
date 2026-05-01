from __future__ import annotations

from pydantic import BaseModel, Field


class MatchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=5, gt=0, le=20)
    threshold: float = Field(default=0.7, ge=0.0, le=1.0)


class SkillMatch(BaseModel):
    skill_id: str
    name: str
    category: str
    similarity: float


class MatchResponse(BaseModel):
    results: list[SkillMatch]
