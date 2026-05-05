-- Skill Intelligence schema (FULL RESET + clean tables)
-- WARNING: This drops the entire `public` schema (CASCADE).

BEGIN;

-- Step 0 — FULL RESET
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- Required for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Restore default privileges to common roles (Supabase-style roles included).
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO anon;
GRANT ALL ON SCHEMA public TO authenticated;
GRANT ALL ON SCHEMA public TO service_role;

-- Optional: allow these roles to use objects created in public.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO postgres, anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO postgres, anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO postgres, anon, authenticated, service_role;

-- Step 1 — skills
CREATE TABLE IF NOT EXISTS public.skills (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  category TEXT,
  source TEXT NOT NULL DEFAULT 'onet',
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Step 2 — occupations
CREATE TABLE IF NOT EXISTS public.occupations (
  id TEXT PRIMARY KEY, -- O*NET code
  title TEXT NOT NULL,
  description TEXT,
  domain TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Step 3 — occupation_skills
CREATE TABLE IF NOT EXISTS public.occupation_skills (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  occupation_id TEXT NOT NULL REFERENCES public.occupations(id) ON DELETE CASCADE,
  skill_id UUID NOT NULL REFERENCES public.skills(id) ON DELETE CASCADE,
  importance FLOAT,
  level FLOAT,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  CONSTRAINT uq_occupation_skill_pair UNIQUE (occupation_id, skill_id)
);

-- Step 5 — Indexes
CREATE INDEX IF NOT EXISTS idx_skills_normalized_name ON public.skills(normalized_name);
CREATE INDEX IF NOT EXISTS idx_occupation_skills_occupation_id ON public.occupation_skills(occupation_id);
CREATE INDEX IF NOT EXISTS idx_occupation_skills_skill_id ON public.occupation_skills(skill_id);

COMMIT;

