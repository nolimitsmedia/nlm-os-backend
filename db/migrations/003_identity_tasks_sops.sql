-- 003_identity_tasks_sops.sql

-- ===== Module 6: Identity =====
CREATE TABLE IF NOT EXISTS users (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email          TEXT NOT NULL UNIQUE,
  name           TEXT NOT NULL,
  role           TEXT NOT NULL DEFAULT 'staff', -- admin | staff | viewer
  password_hash  TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ===== Client Integrations (ClickUp mapping, future SharePoint mapping) =====
CREATE TABLE IF NOT EXISTS client_integrations (
  id         BIGSERIAL PRIMARY KEY,
  client_id  TEXT NOT NULL,
  provider   TEXT NOT NULL, -- 'clickup', 'sharepoint'
  settings   JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, provider)
);

-- ===== Module 4: SOP Reference Layer =====
CREATE TABLE IF NOT EXISTS sops (
  id         BIGSERIAL PRIMARY KEY,
  client_id  TEXT NOT NULL,
  title      TEXT NOT NULL,
  url        TEXT NOT NULL,
  tags       TEXT[] NOT NULL DEFAULT '{}',
  source     TEXT NOT NULL DEFAULT 'manual',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sops_client_id ON sops (client_id);
CREATE INDEX IF NOT EXISTS idx_sops_tags ON sops USING GIN (tags);

-- ===== Module 6: Audit =====
CREATE TABLE IF NOT EXISTS audit_logs (
  id         BIGSERIAL PRIMARY KEY,
  user_id    TEXT,
  action     TEXT NOT NULL,   -- create/update/delete/login/etc
  entity     TEXT NOT NULL,   -- task/sop/client/etc
  entity_id  TEXT,
  client_id  TEXT,
  meta       JSONB NOT NULL DEFAULT '{}'::jsonb,
  ip         TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_client_id ON audit_logs (client_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_logs (created_at DESC);