from __future__ import annotations

from fastapi import APIRouter, Depends
from loguru import logger
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from app.db.session import get_db
from app.schemas.matching import MatchRequest, MatchResponse, SkillMatch
from app.schemas.response import ApiResponse
from app.services.matching.matcher import match_skills as run_match_skills


router = APIRouter()


@router.post("/match-skills", response_model=ApiResponse)
def match_skills_endpoint(
    body: MatchRequest,
    db: Session = Depends(get_db),
) -> ApiResponse | JSONResponse:
    try:
        rows = run_match_skills(
            db,
            body.query,
            top_k=body.top_k,
            threshold=body.threshold,
        )
        results = [SkillMatch(**row) for row in rows]
        payload = MatchResponse(results=results)
        return ApiResponse.ok(payload.model_dump())
    except Exception:
        logger.exception("match-skills | unexpected error")
        return JSONResponse(
            status_code=500,
            content=ApiResponse.fail(message="Something went wrong").model_dump(
                exclude_none=True
            ),
        )
