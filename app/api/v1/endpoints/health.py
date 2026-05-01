from __future__ import annotations

from fastapi import APIRouter

from app.schemas.response import ApiResponse


router = APIRouter()


@router.get("/health", response_model=ApiResponse)
def health() -> ApiResponse:
    return ApiResponse.ok(
        {
            "service": "SkillMap AI Backend",
            "status": "running",
        }
    )

