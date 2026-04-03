// services/api/src/utils/audit.ts
import { query } from "../db.js";

let ensured = false;

export async function ensureAuditTable() {
  if (ensured) return;

  try {
    await query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`).catch(() => null);

    await query(`
      CREATE TABLE IF NOT EXISTS audit_logs (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id text,
        action text NOT NULL,
        entity text NOT NULL,
        entity_id text,
        client_id text,
        meta jsonb NOT NULL DEFAULT '{}'::jsonb,
        ip text,
        created_at timestamptz NOT NULL DEFAULT now()
      )
    `).catch(() => null);

    await query(`
      CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
      ON audit_logs (created_at DESC)
    `).catch(() => null);

    await query(`
      CREATE INDEX IF NOT EXISTS idx_audit_logs_client_id
      ON audit_logs (client_id, created_at DESC)
    `).catch(() => null);

    await query(`
      CREATE INDEX IF NOT EXISTS idx_audit_logs_entity
      ON audit_logs (entity, created_at DESC)
    `).catch(() => null);
  } finally {
    ensured = true;
  }
}

export async function writeAudit(params: {
  user_id: string | null;
  action: string;
  entity: string;
  entity_id: string | null;
  client_id: string | null;
  meta?: any;
  ip?: string | null;
}) {
  const { user_id, action, entity, entity_id, client_id, meta, ip } = params;

  try {
    await ensureAuditTable();
    await query(
      `INSERT INTO audit_logs (user_id, action, entity, entity_id, client_id, meta, ip)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [user_id, action, entity, entity_id, client_id, meta ?? {}, ip ?? null],
    );
  } catch (e: any) {
    console.warn("[audit] write skipped:", e?.message || e);
  }
}
