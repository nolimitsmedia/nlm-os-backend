// services/api/src/routes/sops.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";
import { optionalAuth, requireAuth } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";
import { isSharePointConfigured } from "../integrations/sharepoint.js";

const router: ExpressRouter = Router();

function authReadRequired() {
  return (process.env.AUTH_REQUIRED_READ || "").toLowerCase() === "true";
}

function safeSharePointConfigured() {
  try {
    return !!isSharePointConfigured();
  } catch {
    return false;
  }
}

function isPgUndefinedTable(err: any) {
  return err?.code === "42P01";
}

function isPgUndefinedColumn(err: any) {
  return err?.code === "42703";
}

/**
 * GET /sops?clientId=...
 */
router.get("/", optionalAuth, async (req: any, res) => {
  try {
    if (authReadRequired() && !req.user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const clientId = String(req.query.clientId || "").trim();
    if (!clientId) {
      return res.status(400).json({ ok: false, error: "clientId required" });
    }

    let rows: any[] = [];
    try {
      // Prefer the newer schema that includes `source`
      const r = await query(
        `SELECT id, client_id, title, url, tags, source, created_at
           FROM sops
          WHERE client_id = $1
          ORDER BY created_at DESC`,
        [clientId],
      );
      rows = r.rows || [];
    } catch (e: any) {
      // ✅ If SOP table isn't created yet, do NOT break Client360
      if (isPgUndefinedTable(e)) {
        return res.json({
          ok: true,
          configured: safeSharePointConfigured(),
          sops: [],
          note: "sops table not found yet (run SOP migrations).",
        });
      }

      // ✅ Back-compat: older schema without `source`
      if (isPgUndefinedColumn(e)) {
        const r2 = await query(
          `SELECT id, client_id, title, url, tags, created_at
             FROM sops
            WHERE client_id = $1
            ORDER BY created_at DESC`,
          [clientId],
        );
        rows = (r2.rows || []).map((x: any) => ({ ...x, source: null }));
      } else {
        throw e;
      }
    }

    return res.json({
      ok: true,
      configured: safeSharePointConfigured(),
      sops: rows,
    });
  } catch (e: any) {
    console.error("[sops] list error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to load SOPs" });
  }
});

/**
 * POST /sops
 * Body: { clientId, title, url, tags? }
 */
router.post("/", requireAuth, async (req: any, res) => {
  try {
    const clientId = String(req.body?.clientId || "").trim();
    const title = String(req.body?.title || "").trim();
    const url = String(req.body?.url || "").trim();
    const tags = Array.isArray(req.body?.tags)
      ? req.body.tags.map((t: any) => String(t).trim()).filter(Boolean)
      : [];

    if (!clientId || !title || !url) {
      return res
        .status(400)
        .json({ ok: false, error: "clientId, title, url required" });
    }

    let row: any;

    try {
      // Newer schema (has `source`)
      const r = await query(
        `INSERT INTO sops (client_id, title, url, tags, source)
         VALUES ($1,$2,$3,$4,'manual')
         RETURNING id, client_id, title, url, tags, source, created_at`,
        [clientId, title, url, tags],
      );
      row = r.rows?.[0];
    } catch (e: any) {
      if (isPgUndefinedTable(e)) {
        return res.status(400).json({
          ok: false,
          error:
            "sops table not found yet. Create it / run SOP migrations first.",
        });
      }
      if (isPgUndefinedColumn(e)) {
        // Back-compat: schema without `source`
        const r2 = await query(
          `INSERT INTO sops (client_id, title, url, tags)
           VALUES ($1,$2,$3,$4)
           RETURNING id, client_id, title, url, tags, created_at`,
          [clientId, title, url, tags],
        );
        row = { ...(r2.rows?.[0] || {}), source: null };
      } else {
        throw e;
      }
    }

    await writeAudit({
      user_id: req.user.id,
      action: "create",
      entity: "sop",
      entity_id: String(row?.id),
      client_id: clientId,
      meta: { title, url },
      ip: req.ip,
    });

    return res.status(201).json({ ok: true, sop: row });
  } catch (e: any) {
    console.error("[sops] create error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to add SOP" });
  }
});

/**
 * DELETE /sops/:id
 */
router.delete("/:id", requireAuth, async (req: any, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "id required" });

    let r: any;
    try {
      r = await query<{ client_id: string }>(
        `DELETE FROM sops WHERE id = $1 RETURNING client_id`,
        [id],
      );
    } catch (e: any) {
      if (isPgUndefinedTable(e)) {
        return res.status(400).json({
          ok: false,
          error:
            "sops table not found yet. Create it / run SOP migrations first.",
        });
      }
      throw e;
    }

    if (!r.rows[0]) {
      return res.status(404).json({ ok: false, error: "Not found" });
    }

    await writeAudit({
      user_id: req.user.id,
      action: "delete",
      entity: "sop",
      entity_id: id,
      client_id: String(r.rows[0].client_id || ""),
      meta: {},
      ip: req.ip,
    });

    return res.json({ ok: true });
  } catch (e: any) {
    console.error("[sops] delete error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to delete SOP" });
  }
});

export default router;
