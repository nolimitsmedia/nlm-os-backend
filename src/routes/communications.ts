// services/api/src/routes/communications.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";
import { optionalAuth, requireAuth } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";

const router: ExpressRouter = Router();

function authReadRequired() {
  return (process.env.AUTH_REQUIRED_READ || "").toLowerCase() === "true";
}

function toInt(value: any) {
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function safeTrim(value: any) {
  return String(value ?? "").trim();
}

function isPgUndefinedTable(err: any) {
  return err?.code === "42P01";
}

export async function ensureCommunicationsTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS communications_timeline (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id text NOT NULL,
      type text NOT NULL DEFAULT 'note',
      title text,
      body text NOT NULL,
      direction text,
      channel text,
      source text NOT NULL DEFAULT 'manual',
      source_ref text,
      status text,
      priority text,
      sentiment text,
      created_by text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_communications_timeline_client_created
    ON communications_timeline (client_id, created_at DESC)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_communications_timeline_client_type
    ON communications_timeline (client_id, type)
  `).catch(() => null);
}

export function buildCommunicationSummary(rows: any[]) {
  const items = Array.isArray(rows) ? rows : [];
  const countsByType = items.reduce((acc: Record<string, number>, item) => {
    const key = safeTrim(item?.type || "note").toLowerCase() || "note";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  const countsByChannel = items.reduce((acc: Record<string, number>, item) => {
    const key = safeTrim(item?.channel || "general").toLowerCase() || "general";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  const unresolved = items.filter((item) =>
    [
      "open",
      "pending",
      "follow-up",
      "follow_up",
      "needs-response",
      "needs response",
    ].includes(safeTrim(item?.status).toLowerCase()),
  );

  const urgentItems = items.filter((item) =>
    ["urgent", "high"].includes(safeTrim(item?.priority).toLowerCase()),
  );

  const negativeSignals = items.filter((item) =>
    ["negative", "at-risk", "risk", "frustrated"].includes(
      safeTrim(item?.sentiment).toLowerCase(),
    ),
  );

  const latest = items
    .slice()
    .sort(
      (a, b) =>
        new Date(b?.created_at || 0).getTime() -
        new Date(a?.created_at || 0).getTime(),
    )[0];

  const summaryParts: string[] = [];
  summaryParts.push(
    items.length
      ? `${items.length} communication timeline entr${items.length === 1 ? "y" : "ies"} recorded.`
      : "No communication timeline entries recorded yet.",
  );
  if (Object.keys(countsByType).length) {
    const topTypes = Object.entries(countsByType)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([key, count]) => `${count} ${key}`)
      .join(", ");
    summaryParts.push(`Top activity types: ${topTypes}.`);
  }
  if (unresolved.length) {
    summaryParts.push(
      `${unresolved.length} entr${unresolved.length === 1 ? "y is" : "ies are"} still open or need follow-up.`,
    );
  }
  if (urgentItems.length) {
    summaryParts.push(
      `${urgentItems.length} entr${urgentItems.length === 1 ? "y is" : "ies are"} marked urgent or high priority.`,
    );
  }
  if (negativeSignals.length) {
    summaryParts.push(
      `${negativeSignals.length} entr${negativeSignals.length === 1 ? "y shows" : "ies show"} negative or at-risk sentiment.`,
    );
  }
  if (latest?.created_at) {
    summaryParts.push(
      `Latest communication logged ${new Date(latest.created_at).toLocaleString()}.`,
    );
  }

  const nextActions = [
    unresolved.length
      ? "Review open follow-ups and assign an owner for each response."
      : "",
    urgentItems.length
      ? "Escalate urgent communication items and align them with task owners."
      : "",
    negativeSignals.length
      ? "Address negative sentiment quickly and send a clear client update."
      : "",
    !items.length
      ? "Log the first communication entry so Client 360 can summarize client interactions."
      : "",
  ].filter(Boolean);

  return {
    total: items.length,
    latest_at: latest?.created_at || null,
    unresolved_count: unresolved.length,
    urgent_count: urgentItems.length,
    negative_count: negativeSignals.length,
    counts_by_type: countsByType,
    counts_by_channel: countsByChannel,
    next_actions: nextActions,
    summary: summaryParts.join(" "),
  };
}

function rowToItem(row: any) {
  return {
    id: String(row?.id || ""),
    client_id: String(row?.client_id || ""),
    type: String(row?.type || "note"),
    title: row?.title ? String(row.title) : null,
    body: String(row?.body || ""),
    direction: row?.direction ? String(row.direction) : null,
    channel: row?.channel ? String(row.channel) : null,
    source: row?.source ? String(row.source) : "manual",
    source_ref: row?.source_ref ? String(row.source_ref) : null,
    status: row?.status ? String(row.status) : null,
    priority: row?.priority ? String(row.priority) : null,
    sentiment: row?.sentiment ? String(row.sentiment) : null,
    created_by: row?.created_by ? String(row.created_by) : null,
    created_at: row?.created_at || null,
    updated_at: row?.updated_at || null,
  };
}

router.get("/summary/:clientId", optionalAuth, async (req: any, res) => {
  try {
    if (authReadRequired() && !req.user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    await ensureCommunicationsTable();
    const clientId = safeTrim(req.params.clientId);
    if (!clientId) {
      return res.status(400).json({ ok: false, error: "clientId required" });
    }

    const r = await query(
      `
      SELECT
        id, client_id, type, title, body, direction, channel, source, source_ref,
        status, priority, sentiment, created_by, created_at, updated_at
      FROM communications_timeline
      WHERE client_id = $1
      ORDER BY created_at DESC
      LIMIT 60
      `,
      [clientId],
    ).catch((e: any) => {
      if (isPgUndefinedTable(e)) return { rows: [] as any[] };
      throw e;
    });

    const items = (r.rows || []).map(rowToItem);
    const summary = buildCommunicationSummary(items);

    return res.json({
      ok: true,
      clientId,
      generated_at: new Date().toISOString(),
      summary: summary.summary,
      metrics: {
        total_entries: summary.total,
        unresolved_count: summary.unresolved_count,
        urgent_count: summary.urgent_count,
        negative_count: summary.negative_count,
      },
      next_actions: summary.next_actions,
      counts_by_type: summary.counts_by_type,
      counts_by_channel: summary.counts_by_channel,
      latest_at: summary.latest_at,
    });
  } catch (e: any) {
    console.error("[communications] summary error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to summarize communications timeline",
    });
  }
});

router.get("/:clientId", optionalAuth, async (req: any, res) => {
  try {
    if (authReadRequired() && !req.user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    await ensureCommunicationsTable();

    const clientId = safeTrim(req.params.clientId);
    const limit = Math.min(Math.max(toInt(req.query.limit || 30), 1), 100);
    if (!clientId) {
      return res.status(400).json({ ok: false, error: "clientId required" });
    }

    const r = await query(
      `
      SELECT
        id, client_id, type, title, body, direction, channel, source, source_ref,
        status, priority, sentiment, created_by, created_at, updated_at
      FROM communications_timeline
      WHERE client_id = $1
      ORDER BY created_at DESC
      LIMIT $2
      `,
      [clientId, limit],
    ).catch((e: any) => {
      if (isPgUndefinedTable(e)) return { rows: [] as any[] };
      throw e;
    });

    const items = (r.rows || []).map(rowToItem);
    return res.json({
      ok: true,
      items,
      summary: buildCommunicationSummary(items),
    });
  } catch (e: any) {
    console.error("[communications] list error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to load communications timeline",
    });
  }
});

router.post("/:clientId", requireAuth, async (req: any, res) => {
  try {
    await ensureCommunicationsTable();

    const clientId = safeTrim(req.params.clientId);
    const type = safeTrim(req.body?.type || "note") || "note";
    const title = safeTrim(req.body?.title || "");
    const body = safeTrim(req.body?.body || "");
    const direction = safeTrim(req.body?.direction || "");
    const channel = safeTrim(req.body?.channel || "");
    const source = safeTrim(req.body?.source || "manual") || "manual";
    const sourceRef = safeTrim(req.body?.sourceRef || "");
    const status = safeTrim(req.body?.status || "");
    const priority = safeTrim(req.body?.priority || "");
    const sentiment = safeTrim(req.body?.sentiment || "");

    if (!clientId || !body) {
      return res
        .status(400)
        .json({ ok: false, error: "clientId and body are required" });
    }

    const inserted = await query(
      `
      INSERT INTO communications_timeline (
        client_id, type, title, body, direction, channel, source, source_ref,
        status, priority, sentiment, created_by
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      RETURNING
        id, client_id, type, title, body, direction, channel, source, source_ref,
        status, priority, sentiment, created_by, created_at, updated_at
      `,
      [
        clientId,
        type,
        title || null,
        body,
        direction || null,
        channel || null,
        source,
        sourceRef || null,
        status || null,
        priority || null,
        sentiment || null,
        safeTrim(req.user?.id || ""),
      ],
    );

    const item = rowToItem(inserted.rows?.[0] || {});

    await writeAudit({
      user_id: req.user.id,
      action: "create",
      entity: "communication_timeline",
      entity_id: item.id,
      client_id: clientId,
      meta: {
        type,
        title,
        channel,
        status,
        priority,
        sentiment,
        source,
      },
      ip: req.ip,
    });

    return res.status(201).json({ ok: true, item });
  } catch (e: any) {
    console.error("[communications] create error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to save communication timeline entry",
    });
  }
});

router.patch("/item/:id", requireAuth, async (req: any, res) => {
  try {
    await ensureCommunicationsTable();
    const id = safeTrim(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: "id required" });

    const existing = await query(
      `
      SELECT id, client_id, type, title, body, direction, channel, source, source_ref,
             status, priority, sentiment, created_by, created_at, updated_at
      FROM communications_timeline
      WHERE id = $1
      LIMIT 1
      `,
      [id],
    );

    if (!existing.rows?.[0]) {
      return res
        .status(404)
        .json({ ok: false, error: "Communication entry not found" });
    }

    const prev = rowToItem(existing.rows[0]);
    const updated = await query(
      `
      UPDATE communications_timeline
      SET
        type = COALESCE($2, type),
        title = COALESCE($3, title),
        body = COALESCE($4, body),
        direction = COALESCE($5, direction),
        channel = COALESCE($6, channel),
        source = COALESCE($7, source),
        source_ref = COALESCE($8, source_ref),
        status = COALESCE($9, status),
        priority = COALESCE($10, priority),
        sentiment = COALESCE($11, sentiment),
        updated_at = NOW()
      WHERE id = $1
      RETURNING
        id, client_id, type, title, body, direction, channel, source, source_ref,
        status, priority, sentiment, created_by, created_at, updated_at
      `,
      [
        id,
        safeTrim(req.body?.type || "") || null,
        safeTrim(req.body?.title || "") || null,
        safeTrim(req.body?.body || "") || null,
        safeTrim(req.body?.direction || "") || null,
        safeTrim(req.body?.channel || "") || null,
        safeTrim(req.body?.source || "") || null,
        safeTrim(req.body?.sourceRef || "") || null,
        safeTrim(req.body?.status || "") || null,
        safeTrim(req.body?.priority || "") || null,
        safeTrim(req.body?.sentiment || "") || null,
      ],
    );

    const item = rowToItem(updated.rows?.[0] || {});
    await writeAudit({
      user_id: req.user.id,
      action: "update",
      entity: "communication_timeline",
      entity_id: item.id,
      client_id: item.client_id,
      meta: { before: prev, after: item },
      ip: req.ip,
    });

    return res.json({ ok: true, item });
  } catch (e: any) {
    console.error("[communications] update error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to update communication timeline entry",
    });
  }
});

router.delete("/item/:id", requireAuth, async (req: any, res) => {
  try {
    await ensureCommunicationsTable();
    const id = safeTrim(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: "id required" });

    const existing = await query(
      `SELECT id, client_id FROM communications_timeline WHERE id = $1 LIMIT 1`,
      [id],
    );
    if (!existing.rows?.[0]) {
      return res
        .status(404)
        .json({ ok: false, error: "Communication entry not found" });
    }

    await query(`DELETE FROM communications_timeline WHERE id = $1`, [id]);

    await writeAudit({
      user_id: req.user.id,
      action: "delete",
      entity: "communication_timeline",
      entity_id: id,
      client_id: String(existing.rows[0].client_id || ""),
      meta: { deleted: true },
      ip: req.ip,
    });

    return res.json({ ok: true });
  } catch (e: any) {
    console.error("[communications] delete error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to delete communication timeline entry",
    });
  }
});

export default router;
