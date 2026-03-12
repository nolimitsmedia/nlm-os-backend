// services/api/src/routes/audit.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router: ExpressRouter = Router();

/**
 * GET /audit?clientId=
 * Admin only
 */
router.get("/", requireAuth, requireRole(["admin"]), async (req, res) => {
  try {
    const clientId = String(req.query.clientId || "").trim();
    const params: any[] = [];
    const where: string[] = [];

    if (clientId) {
      params.push(clientId);
      where.push(`client_id = $${params.length}`);
    }

    const sql = `
      SELECT id, user_id, action, entity, entity_id, client_id, meta, ip, created_at
      FROM audit_logs
      ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
      ORDER BY created_at DESC
      LIMIT 250
    `;

    const r = await query(sql, params);
    return res.json({ ok: true, logs: r.rows });
  } catch (e: any) {
    console.error("[audit] list error", e);
    return res
      .status(500)
      .json({ ok: false, error: "Failed to load audit logs" });
  }
});

export default router;
