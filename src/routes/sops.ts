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

function getFallbackState() {
  const configured = safeSharePointConfigured();
  return {
    sharepoint_configured: configured,
    fallback_mode: !configured,
    fallback_reason: configured
      ? null
      : "SharePoint API is not configured yet. Manual SOP references remain active.",
  };
}

function isPgUndefinedTable(err: any) {
  return err?.code === "42P01";
}

function isPgUndefinedColumn(err: any) {
  return err?.code === "42703";
}

function buildPreviewText(row: any) {
  const parts = [
    row?.title ? `Title: ${row.title}` : "",
    row?.url ? `Reference: ${row.url}` : "",
    Array.isArray(row?.tags) && row.tags.length
      ? `Tags: ${row.tags.join(", ")}`
      : "",
    String(row?.source || "").trim()
      ? `Source: ${String(row.source).trim()}`
      : "",
  ].filter(Boolean);

  return parts.join(" • ");
}

function buildSopSummary(rows: any[]) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const manualCount = safeRows.filter(
    (row) => String(row?.source || "manual").toLowerCase() === "manual",
  ).length;
  const sharepointCount = safeRows.filter((row) =>
    String(row?.source || "")
      .toLowerCase()
      .includes("sharepoint"),
  ).length;

  return {
    count: safeRows.length,
    manual_count: manualCount,
    sharepoint_count: sharepointCount,
    recent_titles: safeRows
      .map((row) => String(row?.title || row?.url || ""))
      .filter(Boolean)
      .slice(0, 5),
  };
}

function buildBatch6Meta(rows: any[]) {
  const summary = buildSopSummary(rows);
  const fallback = getFallbackState();

  return {
    ...fallback,
    summary,
    recommendations: [
      !summary.count ? "Add at least one SOP reference for this client." : "",
      fallback.fallback_mode
        ? "Use manual SOP links until SharePoint API is live."
        : "Prefer SharePoint-backed SOP links where possible.",
    ].filter(Boolean),
  };
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
      // If SOP table isn't created yet, do NOT break Client360
      if (isPgUndefinedTable(e)) {
        const fallback = getFallbackState();
        return res.json({
          ok: true,
          configured: fallback.sharepoint_configured,
          sharepoint_configured: fallback.sharepoint_configured,
          fallback_mode: fallback.fallback_mode,
          fallback_reason: fallback.fallback_reason,
          sops: [],
          summary: buildSopSummary([]),
          recommendations: [
            "Run SOP migrations to create the sops table.",
            fallback.fallback_mode
              ? "Use manual SOP links until SharePoint API is live."
              : "Link SharePoint-backed SOP references once available.",
          ],
          note: "sops table not found yet (run SOP migrations).",
        });
      }

      // Back-compat: older schema without `source`
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

    const meta = buildBatch6Meta(rows);

    return res.json({
      ok: true,
      configured: meta.sharepoint_configured,
      sharepoint_configured: meta.sharepoint_configured,
      fallback_mode: meta.fallback_mode,
      fallback_reason: meta.fallback_reason,
      summary: meta.summary,
      recommendations: meta.recommendations,
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
      meta: { title, url, source: row?.source || "manual" },
      ip: req.ip,
    });

    const fallback = getFallbackState();

    return res.status(201).json({
      ok: true,
      configured: fallback.sharepoint_configured,
      sharepoint_configured: fallback.sharepoint_configured,
      fallback_mode: fallback.fallback_mode,
      fallback_reason: fallback.fallback_reason,
      sop: row,
    });
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

    const fallback = getFallbackState();

    return res.json({
      ok: true,
      configured: fallback.sharepoint_configured,
      sharepoint_configured: fallback.sharepoint_configured,
      fallback_mode: fallback.fallback_mode,
      fallback_reason: fallback.fallback_reason,
    });
  } catch (e: any) {
    console.error("[sops] delete error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to delete SOP" });
  }
});

/**
 * GET /sops/summary?clientId=...
 */
router.get("/summary", optionalAuth, async (req: any, res) => {
  try {
    if (authReadRequired() && !req.user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const clientId = String(req.query.clientId || "").trim();
    if (!clientId) {
      return res.status(400).json({ ok: false, error: "clientId required" });
    }

    const list = await query(
      `SELECT id, client_id, title, url, tags, COALESCE(source, 'manual') AS source, created_at
         FROM sops
        WHERE client_id = $1
        ORDER BY created_at DESC`,
      [clientId],
    ).catch((e: any) => {
      if (isPgUndefinedTable(e)) return { rows: [] as any[] };
      throw e;
    });

    const rows = list.rows || [];
    const meta = buildBatch6Meta(rows);

    return res.json({
      ok: true,
      configured: meta.sharepoint_configured,
      sharepoint_configured: meta.sharepoint_configured,
      fallback_mode: meta.fallback_mode,
      fallback_reason: meta.fallback_reason,
      summary: meta.summary,
      recommendations: meta.recommendations,
    });
  } catch (e: any) {
    console.error("[sops] summary error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to load SOP summary" });
  }
});

/**
 * GET /sops/gap-analysis?clientId=...
 */
router.get("/gap-analysis", optionalAuth, async (req: any, res) => {
  try {
    if (authReadRequired() && !req.user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const clientId = String(req.query.clientId || "").trim();
    if (!clientId) {
      return res.status(400).json({ ok: false, error: "clientId required" });
    }

    const sopList = await query(
      `SELECT id, title, url, tags, created_at, COALESCE(source, 'manual') AS source
         FROM sops
        WHERE client_id = $1
        ORDER BY created_at DESC`,
      [clientId],
    ).catch((e: any) => {
      if (isPgUndefinedTable(e)) return { rows: [] as any[] };
      throw e;
    });

    const taskList = await query(
      `SELECT id, title, sop_id, sop_title, sop_url, status, created_at
         FROM tasks
        WHERE client_id = $1
        ORDER BY created_at DESC
        LIMIT 25`,
      [clientId],
    ).catch((e: any) => {
      if (isPgUndefinedTable(e)) return { rows: [] as any[] };
      throw e;
    });

    const sopRows = sopList.rows || [];
    const taskRows = taskList.rows || [];
    const tasksMissingSop = taskRows.filter(
      (task: any) =>
        !String(task?.sop_id || task?.sop_title || task?.sop_url || "").trim(),
    );

    const hasSops = sopRows.length > 0;
    const gapDetected = !hasSops || tasksMissingSop.length > 0;
    const severity = !hasSops
      ? "high"
      : tasksMissingSop.length >= 3
        ? "medium"
        : tasksMissingSop.length >= 1
          ? "low"
          : "none";

    const fallback = getFallbackState();

    return res.json({
      ok: true,
      configured: fallback.sharepoint_configured,
      sharepoint_configured: fallback.sharepoint_configured,
      fallback_mode: fallback.fallback_mode,
      fallback_reason: fallback.fallback_reason,
      has_sops: hasSops,
      gap_detected: gapDetected,
      severity,
      reasons: [
        !hasSops ? "No SOP references are linked to this client yet." : "",
        tasksMissingSop.length
          ? `${tasksMissingSop.length} recent task${tasksMissingSop.length === 1 ? "" : "s"} are missing an SOP link.`
          : "",
        fallback.fallback_mode
          ? "SharePoint API is pending, so manual SOP coverage should be maintained."
          : "",
        fallback.sharepoint_configured && !hasSops
          ? "SharePoint is configured, but no SOP references have been linked."
          : "",
      ].filter(Boolean),
      suggested_actions: [
        !hasSops ? "Add at least one SOP reference for this client." : "",
        tasksMissingSop.length
          ? "Link SOP references to active tasks to improve execution consistency."
          : "",
        fallback.sharepoint_configured
          ? "Use SharePoint-backed SOP links where possible."
          : "Use manual SOP links until SharePoint API is live.",
      ].filter(Boolean),
      summary: {
        sop_count: sopRows.length,
        task_count: taskRows.length,
        tasks_missing_sop: tasksMissingSop.length,
      },
      tasks_missing_sop: tasksMissingSop.map((task: any) => ({
        id: task?.id,
        title: task?.title || "Untitled Task",
        status: task?.status || null,
        created_at: task?.created_at || null,
        sop_gap: true,
        sop_recommendation: hasSops
          ? "Link an existing SOP to this task."
          : "Create or attach an SOP for this client first.",
      })),
    });
  } catch (e: any) {
    console.error("[sops] gap-analysis error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to analyze SOP coverage",
    });
  }
});

/**
 * GET /sops/:id/preview
 */
router.get("/:id/preview", optionalAuth, async (req: any, res) => {
  try {
    if (authReadRequired() && !req.user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "id required" });

    const r = await query(
      `SELECT id, client_id, title, url, tags,
              COALESCE(source, 'manual') AS source,
              created_at
         FROM sops
        WHERE id::text = $1
        LIMIT 1`,
      [id],
    ).catch((e: any) => {
      if (isPgUndefinedTable(e)) return { rows: [] as any[] };
      throw e;
    });

    const row = r.rows?.[0] || null;
    if (!row) {
      return res.status(404).json({ ok: false, error: "SOP not found" });
    }

    const fallback = getFallbackState();

    return res.json({
      ok: true,
      configured: fallback.sharepoint_configured,
      sharepoint_configured: fallback.sharepoint_configured,
      fallback_mode: fallback.fallback_mode,
      fallback_reason: fallback.fallback_reason,
      preview: {
        id: row.id,
        client_id: row.client_id || null,
        title: row.title || null,
        url: row.url || null,
        source: row.source || null,
        tags: Array.isArray(row.tags) ? row.tags : [],
        preview_text: buildPreviewText(row),
      },
    });
  } catch (e: any) {
    console.error("[sops] preview error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to load SOP preview",
    });
  }
});

export default router;
