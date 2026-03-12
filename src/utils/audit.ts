// services/api/src/utils/audit.ts
import { query } from "../db.js";

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

  await query(
    `INSERT INTO audit_logs (user_id, action, entity, entity_id, client_id, meta, ip)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [user_id, action, entity, entity_id, client_id, meta ?? {}, ip ?? null],
  );
}
