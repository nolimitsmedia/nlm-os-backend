// services/api/src/routes/audit.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";
import { requireAuth, requireRole } from "../middleware/auth.js";
import { ensureAuditTable } from "../utils/audit.js";

const router: ExpressRouter = Router();

const AUDIT_ALLOWED_ROLES = ["admin", "operations", "finance", "tech"];

router.get(
  "/",
  requireAuth,
  requireRole(AUDIT_ALLOWED_ROLES),
  async (req, res) => {
    try {
      await ensureAuditTable();

      const clientId = String(req.query.clientId || "").trim();
      const userId = String(req.query.userId || "").trim();
      const entity = String(req.query.entity || "").trim();
      const action = String(req.query.action || "").trim();
      const limitRaw = Number(req.query.limit || 250);
      const limit = Number.isFinite(limitRaw)
        ? Math.max(1, Math.min(500, Math.trunc(limitRaw)))
        : 250;

      const params: any[] = [];
      const where: string[] = [];

      if (clientId) {
        params.push(clientId);
        where.push(`client_id = $${params.length}`);
      }

      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }

      if (entity) {
        params.push(entity);
        where.push(`LOWER(entity) = LOWER($${params.length})`);
      }

      if (action) {
        params.push(action);
        where.push(`LOWER(action) = LOWER($${params.length})`);
      }

      params.push(limit);

      const sql = `
        SELECT id, user_id, action, entity, entity_id, client_id, meta, ip, created_at
        FROM audit_logs
        ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
        ORDER BY created_at DESC
        LIMIT $${params.length}
      `;

      const r = await query(sql, params);
      return res.json({ ok: true, logs: r.rows });
    } catch (e: any) {
      console.error("[audit] list error", e);
      return res
        .status(500)
        .json({ ok: false, error: e?.message || "Failed to load audit logs" });
    }
  },
);

export default router;
