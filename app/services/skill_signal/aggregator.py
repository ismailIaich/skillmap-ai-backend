from __future__ import annotations

from typing import Any

from loguru import logger

from app.services.embedding.encoder import encode_text
from app.services.embedding.similarity import cosine_similarity


def _is_usable_embedding(value: object) -> bool:
    if value is None:
        return False
    if not isinstance(value, (list, tuple)):
        return False
    if len(value) == 0:
        return False
    try:
        for x in value:
            float(x)
    except (TypeError, ValueError):
        return False
    return True


def _normalize_skill(row: object) -> dict[str, Any] | None:
    """Accept extractor-shaped dicts; optional ``embedding`` for dedup."""
    if not isinstance(row, dict):
        return None
    try:
        sid = row.get("skill_id")
        name = row.get("name")
        category = row.get("category")
        sim_raw = row.get("similarity")
        if sid is None or name is None or category is None or sim_raw is None:
            return None
        sim = float(sim_raw)
    except (TypeError, ValueError):
        return None
    out: dict[str, Any] = {
        "skill_id": str(sid),
        "name": str(name),
        "category": str(category),
        "similarity": sim,
    }
    emb = row.get("embedding")
    if _is_usable_embedding(emb):
        out["embedding"] = list(emb)  # type: ignore[arg-type]
    return out


def _dedup_vector(skill: dict[str, Any]) -> list[float]:
    """Same text recipe as DB embeddings when vectors are not supplied."""
    emb = skill.get("embedding")
    if _is_usable_embedding(emb):
        return [float(x) for x in emb]  # type: ignore[union-attr]
    text = f"{skill['name']} | {skill['category']}"
    return encode_text(text)


def aggregate_skills(
    skills: list[dict[str, Any]],
    threshold: float = 0.7,
    max_per_category: int = 2,
    dedup_similarity: float = 0.85,
) -> list[dict[str, Any]]:
    """
    Filter by score, sort, semantic dedupe (vs kept higher scores), then cap per category.

    Dedup uses ``cosine_similarity`` on stored embeddings when present, otherwise
    ``encode_text(name | category)`` to align with how skills were embedded.
    """
    if skills is None:
        return []
    if not isinstance(skills, list):
        logger.warning("aggregate_skills | expected list, got {t}", t=type(skills).__name__)
        return []
    if len(skills) == 0:
        logger.info("aggregate_skills | input skills=0")
        return []

    n_input = len(skills)
    logger.info("aggregate_skills | input skills={n}", n=n_input)

    normalized: list[dict[str, Any]] = []
    for row in skills:
        item = _normalize_skill(row)
        if item is None:
            logger.debug("aggregate_skills | skipping malformed row={row}", row=row)
            continue
        normalized.append(item)

    passed = [s for s in normalized if s["similarity"] >= threshold]
    passed.sort(key=lambda x: x["similarity"], reverse=True)

    n_after_threshold = len(passed)
    logger.info(
        "aggregate_skills | after threshold | count={n} (removed={r})",
        n=n_after_threshold,
        r=len(normalized) - n_after_threshold,
    )

    kept: list[dict[str, Any]] = []
    vectors_kept: list[list[float]] = []

    for cand in passed:
        try:
            v_c = _dedup_vector(cand)
        except Exception:
            logger.warning(
                "aggregate_skills | encode failed for skill_id={sid}; keeping row without dedup tie-break",
                sid=cand.get("skill_id"),
            )
            v_c = []

        duplicate = False
        if v_c:
            for v_k in vectors_kept:
                try:
                    if cosine_similarity(v_c, v_k) > dedup_similarity:
                        duplicate = True
                        break
                except Exception:
                    continue
        if duplicate:
            continue

        clean = {
            "skill_id": cand["skill_id"],
            "name": cand["name"],
            "category": cand["category"],
            "similarity": float(cand["similarity"]),
        }
        kept.append(clean)
        vectors_kept.append(v_c)

    n_after_dedup = len(kept)
    dedup_removed = n_after_threshold - n_after_dedup
    logger.info(
        "aggregate_skills | after dedup | count={n} (dedup_removed={r})",
        n=n_after_dedup,
        r=dedup_removed,
    )

    if max_per_category < 1:
        logger.info(
            "aggregate_skills | final | count={n} (category cap disabled)",
            n=n_after_dedup,
        )
        return kept

    per_cat: dict[str, int] = {}
    balanced: list[dict[str, Any]] = []
    for item in kept:
        cat = item["category"]
        if per_cat.get(cat, 0) >= max_per_category:
            continue
        balanced.append(item)
        per_cat[cat] = per_cat.get(cat, 0) + 1

    logger.info("aggregate_skills | final output | count={n}", n=len(balanced))
    return balanced
