# SkillMap AI Backend

Production-ready FastAPI foundation (clean config, logging, DB session plumbing, and versioned API routing). No business logic yet.

## Requirements

- Python 3.12+
- PostgreSQL connection string

## Environment variables

Required:

- `DATABASE_URL`
- `TAVILY_API_KEY`
- `HF_API_KEY`

Optional:

- `LOG_LEVEL` (default: `INFO`)
- `SQLALCHEMY_ECHO` (default: `false`)

## Install

```bash
python -m venv venv
./venv/bin/pip install -r requirements.txt
```

## Run

```bash
uvicorn app.main:app --reload
```

Health check:

- `GET /api/v1/health`