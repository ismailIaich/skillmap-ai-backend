"""Load and parse O*NET tab-delimited files (skills, occupations, occupation–skill importance).

O*NET text releases place files under a folder such as ``db_30_2_text/``.

- **Skill catalog (detailed):** ``Technology Skills.txt`` lists concrete tools and technologies
  (``Example``) with UNSPSC ``Commodity Code`` / ``Commodity Title`` — the detailed “skills data”
  vocabulary. This is separate from the small set (~35) of content-model *Skills* elements in
  ``Skills.txt`` ratings.
- **Product-ready filter:** ``filter_product_ready_skills`` shrinks the tech catalog using
  ``Skills.txt`` importance (eligible occupations), coverage across occupations, name cleanup,
  and a 300–800 target band.
- **Occupation–skill importance mappings:** unchanged — ``Skills.txt`` (Scale ID ``IM``).
- **Occupations:** ``Occupation Data.txt`` (or ``Occupations.txt``).

This script does not write to the database — it only parses and logs counts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import tempfile
import uuid
import zipfile
from collections import defaultdict
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlretrieve

from loguru import logger

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from app.onet_types import OnetOccupation, OnetOccupationSkill, OnetSkill

# Default release matching current O*NET text DB layout (tab files under db_*_text/).
DEFAULT_ONET_TEXT_ZIP_URL = "https://www.onetcenter.org/dl_files/database/db_30_2_text.zip"

_WS_RE = re.compile(r"\s+")
# Heuristic: skill names that are mostly noise for product UX.
_NOISY_URL_RE = re.compile(r"https?://|www\.", re.I)
_NOISY_RUN_DIGITS_RE = re.compile(r"\d{5,}")


def _collapse_ws(value: str) -> str:
    return _WS_RE.sub(" ", value.strip())


def _normalize_id(value: str) -> str:
    return value.strip()


def _normalize_free_text(value: str, *, lowercase: bool) -> str:
    s = _collapse_ws(value)
    return s.lower() if lowercase else s


def _normalize_name_key(value: str) -> str:
    """Strip, collapse whitespace, lowercase — for duplicate detection only."""
    return _normalize_free_text(value, lowercase=True)


def _stable_tech_skill_id(norm_key: str) -> str:
    """Deterministic id from dedupe key (Technology Skills have no global Element ID)."""
    h = hashlib.sha256(norm_key.encode("utf-8")).hexdigest()[:16]
    return f"onet-tech:{h}"


def filtered_technology_edges(
    tech_rows: list[dict[str, str]],
    filtered_skills: list[OnetSkill],
    *,
    qualified_occupations: set[str] | None = None,
) -> list[tuple[str, str]]:
    """
    Build unique (occupation_id, normalized_skill_name) rows from Technology Skills.

    - Only keep edges whose Example maps to a filtered skill name (normalized).
    - Optionally restrict to occupations in ``qualified_occupations``.
    """
    allowed = {s.name for s in filtered_skills}
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for row in tech_rows:
        code = row.get("O*NET-SOC Code") or row.get("ONET-SOC Code")
        raw_ex = row.get("Example")
        if not code or raw_ex is None or not str(raw_ex).strip():
            continue
        occ = _normalize_id(str(code))
        if qualified_occupations is not None and occ not in qualified_occupations:
            continue
        nk = _normalize_name_key(_collapse_ws(str(raw_ex)))
        if nk not in allowed:
            continue
        key = (occ, nk)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def qualified_occupations_for_importance(
    mappings: list[OnetOccupationSkill], *, min_importance: float
) -> set[str]:
    """Occupations that have at least one Skills.txt IM rating at or above ``min_importance``."""
    out: set[str] = set()
    for m in mappings:
        if m.importance >= min_importance:
            out.add(m.occupation_id)
    return out


def tech_example_norm_key_to_occupations(
    tech_rows: list[dict[str, str]], qualified_occupations: set[str]
) -> dict[str, set[str]]:
    """
    Map normalized Example name → distinct occupations (restricted to ``qualified_occupations``).

    Technology Skills rows are the occupation–technology “edges”; there is no separate importance
    column, so we scope to occupations that already have meaningful IM scores in ``Skills.txt``.
    """
    by_key: dict[str, set[str]] = defaultdict(set)
    for row in tech_rows:
        code = row.get("O*NET-SOC Code") or row.get("ONET-SOC Code")
        raw_ex = row.get("Example")
        if not code or raw_ex is None or not str(raw_ex).strip():
            continue
        occ = _normalize_id(code)
        if occ not in qualified_occupations:
            continue
        display = _collapse_ws(str(raw_ex))
        nk = _normalize_name_key(display)
        by_key[nk].add(occ)
    return by_key


def _is_noisy_skill_name(name: str) -> bool:
    if not name or len(name) < 2:
        return True
    letters = sum(1 for c in name if c.isalpha())
    if letters < 2:
        return True
    if _NOISY_URL_RE.search(name):
        return True
    if _NOISY_RUN_DIGITS_RE.search(name):
        return True
    alnum_ratio = sum(1 for c in name if c.isalnum() or c.isspace()) / len(name)
    if alnum_ratio < 0.55:
        return True
    words = name.split()
    if len(words) > 14:
        return True
    return False


def filter_product_ready_skills(
    skills: list[OnetSkill],
    tech_rows: list[dict[str, str]],
    mappings: list[OnetOccupationSkill],
    *,
    im_threshold: float = 3.0,
    min_occupations: int = 5,
    max_name_len: int = 64,
    target_min: int = 300,
    target_max: int = 800,
) -> list[OnetSkill]:
    """
    Reduce the technology skill catalog using occupation coverage, IM thresholds (via eligible
    occupations), name quality, and a 300–800 target band (frequency-ranked cap / relax min coverage).
    """
    initial = len(skills)
    qualified = qualified_occupations_for_importance(mappings, min_importance=im_threshold)
    occ_by_key = tech_example_norm_key_to_occupations(tech_rows, qualified)

    def collect_with_min_mo(mo: int) -> list[tuple[int, OnetSkill]]:
        rows: list[tuple[int, OnetSkill]] = []
        for s in skills:
            nk = _normalize_name_key(s.name)
            cnt = len(occ_by_key.get(nk, set()))
            if cnt < mo:
                continue
            if len(s.name) > max_name_len:
                continue
            if _is_noisy_skill_name(s.name):
                continue
            rows.append((cnt, s))
        rows.sort(key=lambda x: (-x[0], x[1].skill_id))
        return rows

    mo = max(1, min_occupations)
    ranked = collect_with_min_mo(mo)
    while len(ranked) < target_min and mo > 1:
        mo -= 1
        ranked = collect_with_min_mo(mo)

    out_skills = [s for _, s in ranked]
    if len(out_skills) > target_max:
        out_skills = out_skills[:target_max]

    removed = initial - len(out_skills)
    logger.info("onet | product skills | initial={n}", n=initial)
    logger.info("onet | product skills | filtered={n}", n=len(out_skills))
    logger.info("onet | product skills | removed={n}", n=removed)

    return out_skills


def read_tsv_dicts(path: Path) -> list[dict[str, str]]:
    """Read a tab-delimited file with a header row into dict rows."""
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for raw in reader:
            row = {k: (v or "").strip() for k, v in raw.items() if k is not None}
            rows.append(row)
    return rows


def parse_occupations(rows: list[dict[str, str]]) -> list[OnetOccupation]:
    """Occupation Data: O*NET-SOC Code, Title, Description (description ignored)."""
    out: list[OnetOccupation] = []
    for row in rows:
        code = row.get("O*NET-SOC Code") or row.get("ONET-SOC Code")
        title = row.get("Title")
        if not code or title is None:
            continue
        out.append(
            OnetOccupation(
                occupation_id=_normalize_id(code),
                title=_normalize_free_text(title, lowercase=False),
            )
        )
    return out


def parse_skill_occupation_mappings(rows: list[dict[str, str]]) -> list[OnetOccupationSkill]:
    """
    Occupation Skills file: O*NET-SOC Code, Element ID, Element Name, Scale ID, Data Value, ...
    Keep Scale ID == IM (importance) only.
    """
    out: list[OnetOccupationSkill] = []
    for row in rows:
        if (row.get("Scale ID") or "").strip() != "IM":
            continue
        code = row.get("O*NET-SOC Code") or row.get("ONET-SOC Code")
        eid = row.get("Element ID")
        raw_val = row.get("Data Value")
        if not code or not eid or raw_val is None or not str(raw_val).strip():
            continue
        try:
            importance = float(str(raw_val).strip())
        except ValueError:
            continue
        out.append(
            OnetOccupationSkill(
                occupation_id=_normalize_id(code),
                skill_id=_normalize_id(eid),
                importance=importance,
            )
        )
    return out


def parse_technology_skills_catalog(rows: list[dict[str, str]]) -> list[OnetSkill]:
    """
    Technology Skills: Example (tool/product), Commodity Code, Commodity Title.

    Dedupe by normalized Example name; description lists distinct commodity titles seen for that
    name (usually one).
    """
    display_by_key: dict[str, str] = {}
    titles_by_key: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        raw_ex = row.get("Example")
        if raw_ex is None or not str(raw_ex).strip():
            continue
        title = row.get("Commodity Title")
        if title is None:
            continue
        display_name = _collapse_ws(str(raw_ex))
        norm_key = _normalize_name_key(display_name)
        if norm_key not in display_by_key:
            display_by_key[norm_key] = display_name
        titles_by_key[norm_key].add(_normalize_free_text(str(title), lowercase=True))

    skills: list[OnetSkill] = []
    for norm_key in sorted(display_by_key.keys()):
        desc = "; ".join(sorted(titles_by_key[norm_key]))
        skills.append(
            OnetSkill(
                skill_id=_stable_tech_skill_id(norm_key),
                name=_normalize_free_text(display_by_key[norm_key], lowercase=True),
                description=desc,
            )
        )
    skills.sort(key=lambda s: s.skill_id)
    return skills


def parse_core_skill_scores(
    rows: list[dict[str, str]],
) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, str]]:
    """
    Parse O*NET core Skills.txt into scored edges.

    Returns:
    - scores: (occupation_id, normalized_skill_name) -> {"importance": float?, "level": float?}
    - display_name_by_norm: normalized_skill_name -> a representative display name
    """
    scores: dict[tuple[str, str], dict[str, float]] = {}
    display_name_by_norm: dict[str, str] = {}

    for row in rows:
        code = row.get("O*NET-SOC Code") or row.get("ONET-SOC Code")
        name = row.get("Element Name")
        scale = (row.get("Scale ID") or "").strip()
        raw_val = row.get("Data Value")

        if not code or not name or not scale or raw_val is None or not str(raw_val).strip():
            continue

        if scale not in {"IM", "LV"}:
            continue

        try:
            val = float(str(raw_val).strip())
        except ValueError:
            continue

        occ = _normalize_id(str(code))
        display = _collapse_ws(str(name))
        norm = _normalize_name_key(display)
        display_name_by_norm.setdefault(norm, display)

        key = (occ, norm)
        obj = scores.setdefault(key, {})
        if scale == "IM":
            obj["importance"] = val
        else:
            obj["level"] = val

    return scores, display_name_by_norm


def first_existing(root: Path, names: tuple[str, ...]) -> Path | None:
    for name in names:
        found = next(root.rglob(name), None)
        if found is not None and found.is_file():
            return found
    return None


def discover_paths(data_dir: Path) -> tuple[Path, Path, Path]:
    """Resolve occupation list, Skills ratings (mappings), and Technology Skills (skill catalog)."""
    occ = first_existing(data_dir, ("Occupation Data.txt", "Occupations.txt"))
    skills_map = first_existing(data_dir, ("Skills.txt",))
    tech_skills = first_existing(data_dir, ("Technology Skills.txt",))
    missing = [
        label
        for label, p in (
            ("Occupation Data.txt or Occupations.txt", occ),
            ("Skills.txt (occupation-level ratings)", skills_map),
            ("Technology Skills.txt (detailed skill catalog)", tech_skills),
        )
        if p is None
    ]
    if missing:
        raise FileNotFoundError(
            "Under {!r}, could not find: {}".format(data_dir, "; ".join(missing))
        )
    assert occ is not None and skills_map is not None and tech_skills is not None
    return occ, skills_map, tech_skills


def download_onet_text_zip(url: str, dest_dir: Path) -> Path:
    """Download the O*NET text DB zip and extract it under ``dest_dir``."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.info("onet | downloading | url={url}", url=url)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        urlretrieve(url, tmp_path)
        with zipfile.ZipFile(tmp_path, "r") as zf:
            zf.extractall(dest_dir)
        logger.info("onet | extracted | dir={dir}", dir=str(dest_dir))
    finally:
        tmp_path.unlink(missing_ok=True)
    return dest_dir


def load_onet_dataset(
    data_dir: Path,
    *,
    im_threshold: float = 3.0,
    min_occupations: int = 5,
    max_name_len: int = 64,
    target_min: int = 300,
    target_max: int = 800,
) -> tuple[
    list[OnetSkill],
    list[OnetOccupation],
    list[OnetOccupationSkill],
    list[dict[str, str]],
    dict[tuple[str, str], dict[str, float]],
]:
    occ_path, skills_path, tech_path = discover_paths(data_dir)

    occ_rows = read_tsv_dicts(occ_path)
    occupations = parse_occupations(occ_rows)

    map_rows = read_tsv_dicts(skills_path)
    mappings = parse_skill_occupation_mappings(map_rows)
    core_scores, core_display = parse_core_skill_scores(map_rows)

    tech_rows = read_tsv_dicts(tech_path)
    skills_raw = parse_technology_skills_catalog(tech_rows)
    skills = filter_product_ready_skills(
        skills_raw,
        tech_rows,
        mappings,
        im_threshold=im_threshold,
        min_occupations=min_occupations,
        max_name_len=max_name_len,
        target_min=target_min,
        target_max=target_max,
    )

    # Ensure core Skills are available as skills too (so scoring/matching can work).
    core_skills = [
        OnetSkill(
            skill_id=f"onet-core:{norm}",
            name=_normalize_name_key(core_display.get(norm, norm)),
            description="",
        )
        for norm in sorted(core_display.keys())
    ]

    merged_skills_by_norm: dict[str, OnetSkill] = {s.name: s for s in skills}
    for s in core_skills:
        merged_skills_by_norm.setdefault(s.name, s)

    merged_skills = list(merged_skills_by_norm.values())
    merged_skills.sort(key=lambda s: s.skill_id)

    return merged_skills, occupations, mappings, tech_rows, core_scores


def seed_supabase(
    *,
    database_url: str,
    skills: list[OnetSkill],
    occupations: list[OnetOccupation],
    occupation_skill_edges: list[tuple[str, str]],
    core_scores: dict[tuple[str, str], dict[str, float]],
    batch_size: int = 750,
) -> tuple[int, int, int, int, int]:
    """
    Insert into Supabase Postgres using DATABASE_URL (no SQLAlchemy).

    Returns (skills_inserted, core_skills_added, occupations_inserted, mappings_scored, mappings_total).
    """
    if not database_url.strip():
        raise ValueError("DATABASE_URL is not set. Please configure your environment.")

    # 1) Skills: generate UUIDs for missing normalized_name, insert missing only.
    skill_norms = [s.name for s in skills]  # already normalized/lowercase
    skill_name_by_norm = {s.name: s.name for s in skills}
    skill_id_by_norm: dict[str, str] = {}
    core_norms = {norm for (_, norm) in core_scores.keys()}

    occ_ids = [o.occupation_id for o in occupations]

    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            # Fetch existing skills (normalized_name unique)
            cur.execute(
                "SELECT normalized_name, id FROM public.skills WHERE normalized_name = ANY(%s)",
                (skill_norms,),
            )
            for norm, sid in cur.fetchall():
                skill_id_by_norm[str(norm)] = str(sid)

            missing_norms = [n for n in skill_norms if n not in skill_id_by_norm]
            missing_core_norms = [n for n in missing_norms if n in core_norms]
            to_insert_skills = [
                (  # id, name, normalized_name, category, source
                    str(uuid.uuid4()),
                    skill_name_by_norm[n],
                    n,
                    ("O*NET Core Skill" if n in core_norms else "O*NET Technology"),
                    "onet",
                )
                for n in missing_norms
            ]

            skills_inserted = 0
            core_skills_added = 0
            if to_insert_skills:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.skills (id, name, normalized_name, category, source)
                    VALUES %s
                    ON CONFLICT (normalized_name) DO NOTHING
                    RETURNING normalized_name, id
                    """,
                    to_insert_skills,
                    page_size=batch_size,
                )
                returned = cur.fetchall()
                skills_inserted = len(returned)
                for norm, sid in returned:
                    skill_id_by_norm[str(norm)] = str(sid)
                core_skills_added = sum(1 for norm, _sid in returned if str(norm) in set(missing_core_norms))

            # 2) Occupations: insert missing only on PK id
            cur.execute(
                "SELECT id FROM public.occupations WHERE id = ANY(%s)",
                (occ_ids,),
            )
            existing_occ = {str(r[0]) for r in cur.fetchall()}
            to_insert_occ = [
                (o.occupation_id, o.title, None, None)  # id,title,description,domain
                for o in occupations
                if o.occupation_id not in existing_occ
            ]
            occupations_inserted = 0
            if to_insert_occ:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.occupations (id, title, description, domain)
                    VALUES %s
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """,
                    to_insert_occ,
                    page_size=batch_size,
                )
                occupations_inserted = len(cur.fetchall())

            # 3) occupation_skills: only edges whose skill exists in filtered set
            # Merge:
            # - Core Skills provide (importance, level) per occupation+skill name
            # - Tech Skills edges add additional occupation+skill pairs (no scores)
            merged: dict[tuple[str, str], tuple[float | None, float | None]] = {}
            for (occ_id, norm_name), vals in core_scores.items():
                imp = vals.get("importance")
                lvl = vals.get("level")
                merged[(occ_id, norm_name)] = (imp, lvl)

            for occ_id, norm_name in occupation_skill_edges:
                merged.setdefault((occ_id, norm_name), (None, None))

            mapping_rows: list[tuple[str, str, float | None, float | None]] = []
            skipped = 0
            scored = 0
            for (occ_id, skill_norm), (imp, lvl) in merged.items():
                sid = skill_id_by_norm.get(skill_norm)
                if sid is None:
                    skipped += 1
                    continue
                if imp is not None or lvl is not None:
                    scored += 1
                mapping_rows.append((occ_id, sid, imp, lvl))

            if skipped:
                logger.warning("onet | seed | skipped edges (missing skill id) | count={n}", n=skipped)

            mappings_total = len(mapping_rows)
            mappings_scored = scored
            mappings_inserted = 0
            if mapping_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.occupation_skills (occupation_id, skill_id, importance, level)
                    VALUES %s
                    ON CONFLICT (occupation_id, skill_id) DO NOTHING
                    RETURNING occupation_id
                    """,
                    mapping_rows,
                    page_size=batch_size,
                )
                mappings_inserted = len(cur.fetchall())

            logger.info("onet | seed | skills inserted | count={n}", n=skills_inserted)
            logger.info("onet | seed | core skills added | count={n}", n=core_skills_added)
            logger.info("onet | seed | occupations inserted | count={n}", n=occupations_inserted)
            logger.info("onet | seed | mappings with importance/level | count={n}", n=mappings_scored)
            logger.info("onet | seed | mappings total (attempted) | count={n}", n=mappings_total)
            logger.info("onet | seed | mappings inserted | count={n}", n=mappings_inserted)

            return (
                skills_inserted,
                core_skills_added,
                occupations_inserted,
                mappings_scored,
                mappings_total,
            )


def main() -> None:
    # Load .env (if present) so DATABASE_URL etc are available.
    load_dotenv()

    parser = argparse.ArgumentParser(description="Parse O*NET text files and log record counts.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/onet"),
        help="Directory containing extracted O*NET text files (searched recursively).",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the official O*NET text database zip into --data-dir before parsing.",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_ONET_TEXT_ZIP_URL,
        help="Zip URL when --download is set.",
    )
    parser.add_argument(
        "--im-threshold",
        type=float,
        default=3.0,
        help="Min Skills.txt importance (1–5 IM scale) for an occupation to count toward tech coverage.",
    )
    parser.add_argument(
        "--min-occupations",
        type=int,
        default=5,
        help="Min distinct qualifying occupations per technology skill (relaxed if needed to reach target).",
    )
    parser.add_argument(
        "--max-name-len",
        type=int,
        default=64,
        help="Drop technology skills with names longer than this (after normalization).",
    )
    parser.add_argument(
        "--target-min",
        type=int,
        default=300,
        help="Desired minimum product skill count (may not be reached if data is sparse).",
    )
    parser.add_argument(
        "--target-max",
        type=int,
        default=800,
        help="Hard cap on product skill count (keeps highest-coverage skills first).",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Insert filtered skills, occupations, and mappings into Supabase Postgres (DATABASE_URL).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=750,
        help="Batch size for inserts (500–1000 recommended).",
    )
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    if args.download:
        try:
            download_onet_text_zip(args.url, data_dir)
        except (OSError, URLError, zipfile.BadZipFile) as e:
            logger.exception("onet | download failed | error={!s}", e)
            raise SystemExit(1) from e
    elif not data_dir.exists():
        logger.error(
            "onet | data dir missing | path={path} | use --download or extract files manually",
            path=str(data_dir),
        )
        raise SystemExit(1)

    try:
        skills, occupations, mappings, tech_rows, core_scores = load_onet_dataset(
            data_dir,
            im_threshold=args.im_threshold,
            min_occupations=args.min_occupations,
            max_name_len=args.max_name_len,
            target_min=args.target_min,
            target_max=args.target_max,
        )
    except FileNotFoundError as e:
        logger.error("onet | {!s}", e)
        raise SystemExit(1) from e

    qualified = qualified_occupations_for_importance(mappings, min_importance=args.im_threshold)
    edges = filtered_technology_edges(
        tech_rows,
        [s for s in skills if not s.skill_id.startswith("onet-core:")],
        qualified_occupations=qualified,
    )
    logger.info("onet | occupations loaded | count={n}", n=len(occupations))
    logger.info("onet | core skill mappings (scored) | count={n}", n=len(core_scores))
    logger.info("onet | technology mappings (unscored) | count={n}", n=len(edges))

    if args.persist:
        db_url = (os.getenv("DATABASE_URL") or "").strip()
        if not db_url:
            raise ValueError("DATABASE_URL is not set. Please configure your environment.")
        logger.info("onet | seed | connecting to database...")
        try:
            skills_i, core_added, occ_i, scored_i, total_i = seed_supabase(
                database_url=db_url,
                skills=skills,
                occupations=occupations,
                occupation_skill_edges=edges,
                core_scores=core_scores,
                batch_size=args.batch_size,
            )
        except Exception:
            logger.exception("onet | seed | failed")
            raise SystemExit(1) from None

        print(f"skills inserted: {skills_i}")
        print(f"core skills added: {core_added}")
        print(f"occupations inserted: {occ_i}")
        print(f"mappings with importance/level: {scored_i}")
        print(f"mappings total: {total_i}")


if __name__ == "__main__":
    main()
