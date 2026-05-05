"""Persist filtered O*NET technology skills, occupations, and skill–occupation links to PostgreSQL."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator, Sequence

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.occupation import Occupation
from app.models.skill import Skill
from app.models.skill_occupation import SkillOccupation
from app.onet_types import OnetOccupation, OnetSkill

CHUNK = 500
ONET_SKILL_CATEGORY = "O*NET Technology"
ONET_OCCUPATION_SECTOR = "O*NET"

_WS_RE = re.compile(r"\s+")


def _collapse_ws(value: str) -> str:
    return _WS_RE.sub(" ", value.strip())


def _example_norm_key(raw: str) -> str:
    """Match catalog skill names (normalized lowercase)."""
    return _collapse_ws(raw).lower()


def collect_filtered_technology_edges(
    tech_rows: list[dict[str, str]],
    filtered_skills: Sequence[OnetSkill],
) -> list[tuple[str, str]]:
    """
    Unique (O*NET-SOC code, skill name) pairs from Technology Skills rows whose Example matches a
    filtered catalog skill (by normalized name). Content-model ``Skills.txt`` mappings use element
    IDs and are not used here.
    """
    allowed = {s.name for s in filtered_skills}
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for row in tech_rows:
        code = row.get("O*NET-SOC Code") or row.get("ONET-SOC Code")
        raw_ex = row.get("Example")
        if not code or raw_ex is None or not str(raw_ex).strip():
            continue
        soc = str(code).strip()
        nk = _example_norm_key(str(raw_ex))
        if nk not in allowed:
            continue
        key = (soc, nk)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _chunks(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def persist_onet_to_db(
    session: Session,
    skills: Sequence[OnetSkill],
    occupations: Sequence[OnetOccupation],
    tech_rows: list[dict[str, str]],
) -> None:
    """
    Upsert filtered skills and occupations, then insert skill–occupation rows for technology edges
    that reference only filtered skills. Wrapped in an outer transaction by the caller.
    """
    edges = collect_filtered_technology_edges(tech_rows, skills)

    skill_rows = [
        {"name": s.name[:255], "category": ONET_SKILL_CATEGORY}
        for s in skills
    ]
    occ_rows = [
        {
            "name": o.title[:255],
            "isco_code": o.occupation_id[:64],
            "sector": ONET_OCCUPATION_SECTOR,
        }
        for o in occupations
    ]

    skill_table = Skill.__table__
    occ_table = Occupation.__table__
    so_table = SkillOccupation.__table__

    for chunk in _chunks(skill_rows, CHUNK):
        stmt = pg_insert(skill_table).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"category": stmt.excluded.category},
        )
        session.execute(stmt)

    # Occupation.isco_code is not unique in the model, so we cannot rely on a DB-level
    # ON CONFLICT target. Instead: bulk insert only missing codes.
    codes = [o.occupation_id[:64] for o in occupations]
    existing_codes = {
        code
        for (code,) in session.execute(
            select(Occupation.isco_code).where(Occupation.isco_code.in_(codes))
        ).all()
    }
    occ_rows_to_insert = [r for r in occ_rows if r["isco_code"] not in existing_codes]
    for chunk in _chunks(occ_rows_to_insert, CHUNK):
        session.execute(pg_insert(occ_table).values(chunk))

    names = [s.name[:255] for s in skills]

    name_to_id = {
        name: sid
        for sid, name in session.execute(
            select(Skill.id, Skill.name).where(Skill.name.in_(names))
        ).all()
    }
    code_to_id = {
        code: oid
        for oid, code in session.execute(
            select(Occupation.id, Occupation.isco_code).where(Occupation.isco_code.in_(codes))
        ).all()
    }

    junction_values: list[dict[str, object]] = []
    skipped = 0
    for soc, sk_name in edges:
        sid = name_to_id.get(sk_name)
        oid = code_to_id.get(soc)
        if sid is None or oid is None:
            skipped += 1
            continue
        junction_values.append(
            {
                "id": uuid.uuid4(),
                "skill_id": sid,
                "occupation_id": oid,
                "weight": 1.0,
            }
        )

    if skipped:
        logger.warning(
            "onet | db | skill_occupation edges skipped (missing fk lookup) | count={n}",
            n=skipped,
        )

    for chunk in _chunks(junction_values, CHUNK):
        stmt = pg_insert(so_table).values(chunk).on_conflict_do_nothing(
            constraint="uq_skill_occupation_pair",
        )
        session.execute(stmt)

    logger.info("onet | db | skills upserted | count={n}", n=len(skills))
    logger.info(
        "onet | db | occupations inserted (missing only) | count={n}",
        n=len(occ_rows_to_insert),
    )
    logger.info(
        "onet | db | skill_occupation rows inserted (attempted) | count={n}",
        n=len(junction_values),
    )
