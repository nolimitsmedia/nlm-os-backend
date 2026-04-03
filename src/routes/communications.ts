// services/api/src/routes/communications.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";
import { optionalAuth, requireAuth } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";

const router: ExpressRouter = Router();

const UNRESOLVED_STATUSES = new Set([
  "open",
  "pending",
  "follow-up",
  "follow_up",
  "needs-response",
  "needs response",
  "needs_response",
]);

const URGENT_PRIORITIES = new Set(["urgent", "high"]);
const NEGATIVE_SENTIMENTS = new Set([
  "negative",
  "at-risk",
  "risk",
  "frustrated",
  "concerned",
]);
const RISK_LEVELS = new Set(["low", "medium", "high", "critical"]);

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

function parseBoolean(value: any, fallback = false) {
  if (typeof value === "boolean") return value;
  const v = safeTrim(value).toLowerCase();
  if (!v) return fallback;
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return fallback;
}

function parseDateOrNull(value: any) {
  const raw = safeTrim(value);
  if (!raw) return null;
  const d = new Date(raw);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function normalizeType(value: any) {
  const raw = safeTrim(value).toLowerCase().replace(/\s+/g, "_");
  if (!raw) return "note";
  const aliasMap: Record<string, string> = {
    note: "note",
    email: "email",
    sms: "sms",
    text: "sms",
    call: "call",
    phone: "call",
    meeting: "meeting",
    voicemail: "voicemail",
    internal: "internal_update",
    internal_update: "internal_update",
    campaign: "campaign_touchpoint",
    campaign_touchpoint: "campaign_touchpoint",
    follow_up: "follow_up",
    followup: "follow_up",
    escalation: "escalation",
  };
  return aliasMap[raw] || raw;
}

function normalizeRiskLevel(value: any) {
  const raw = safeTrim(value).toLowerCase().replace(/\s+/g, "_");
  if (!raw) return null;
  if (raw === "at_risk") return "high";
  return RISK_LEVELS.has(raw) ? raw : null;
}

async function getColumnSet(tableName: string) {
  const r = await query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = $1
    `,
    [tableName],
  ).catch(() => ({ rows: [] as any[] }));

  return new Set(
    (r.rows || []).map((row: any) => String(row.column_name || "").trim()),
  );
}

function selectCol(
  cols: Set<string>,
  column: string,
  fallbackSql: string,
  alias = column,
) {
  return cols.has(column)
    ? `${column} AS ${alias}`
    : `${fallbackSql} AS ${alias}`;
}

export async function ensureCommunicationsTable() {
  await query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`).catch(() => null);

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

  const alterStatements = [
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS follow_up_due_at timestamptz`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS owner_id text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS owner_name text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS owner_email text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS needs_response boolean NOT NULL DEFAULT false`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS is_pinned boolean NOT NULL DEFAULT false`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS pinned_at timestamptz`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS pinned_by text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS converted_from_id text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS related_task_id text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS related_note_id text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS escalation_level text`,
    `ALTER TABLE communications_timeline ADD COLUMN IF NOT EXISTS risk_level text`,
  ];

  for (const statement of alterStatements) {
    await query(statement).catch((e: any) => {
      console.warn("[communications] schema patch skipped:", e?.message || e);
      return null;
    });
  }

  await query(`
    CREATE INDEX IF NOT EXISTS idx_communications_timeline_client_created
    ON communications_timeline (client_id, created_at DESC)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_communications_timeline_client_type
    ON communications_timeline (client_id, type)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_communications_timeline_client_pinned
    ON communications_timeline (client_id, is_pinned, created_at DESC)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_communications_timeline_client_follow_up_due
    ON communications_timeline (client_id, follow_up_due_at)
  `).catch(() => null);
}

function computeSummaryParts(rows: any[], label: string) {
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

  const unresolved = items.filter((item) => {
    const status = safeTrim(item?.status).toLowerCase();
    return Boolean(item?.needs_response) || UNRESOLVED_STATUSES.has(status);
  });

  const urgentItems = items.filter((item) => {
    const priority = safeTrim(item?.priority).toLowerCase();
    const riskLevel = safeTrim(item?.risk_level).toLowerCase();
    return URGENT_PRIORITIES.has(priority) || riskLevel === "critical";
  });

  const negativeSignals = items.filter((item) => {
    const sentiment = safeTrim(item?.sentiment).toLowerCase();
    const riskLevel = safeTrim(item?.risk_level).toLowerCase();
    return (
      NEGATIVE_SENTIMENTS.has(sentiment) ||
      ["high", "critical"].includes(riskLevel)
    );
  });

  const pinnedItems = items.filter((item) => Boolean(item?.is_pinned));
  const needsResponseItems = items.filter((item) =>
    Boolean(item?.needs_response),
  );
  const overdueFollowups = unresolved.filter((item) => {
    const due = item?.follow_up_due_at
      ? new Date(item.follow_up_due_at).getTime()
      : NaN;
    return Number.isFinite(due) && due < Date.now();
  });

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
      ? `${label}: ${items.length} communication entr${items.length === 1 ? "y" : "ies"} recorded.`
      : `${label}: no communication entries recorded yet.`,
  );
  if (Object.keys(countsByType).length) {
    const topTypes = Object.entries(countsByType)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([key, count]) => `${count} ${key.replace(/_/g, " ")}`)
      .join(", ");
    summaryParts.push(`Top activity types: ${topTypes}.`);
  }
  if (unresolved.length) {
    summaryParts.push(
      `${unresolved.length} entr${unresolved.length === 1 ? "y still needs" : "ies still need"} follow-up or a response.`,
    );
  }
  if (needsResponseItems.length) {
    summaryParts.push(
      `${needsResponseItems.length} entr${needsResponseItems.length === 1 ? "y is" : "ies are"} marked needs response.`,
    );
  }
  if (overdueFollowups.length) {
    summaryParts.push(
      `${overdueFollowups.length} follow-up${overdueFollowups.length === 1 ? " is" : "s are"} overdue.`,
    );
  }
  if (urgentItems.length) {
    summaryParts.push(
      `${urgentItems.length} entr${urgentItems.length === 1 ? "y is" : "ies are"} urgent or critical.`,
    );
  }
  if (negativeSignals.length) {
    summaryParts.push(
      `${negativeSignals.length} entr${negativeSignals.length === 1 ? "y shows" : "ies show"} negative or high-risk sentiment.`,
    );
  }
  if (pinnedItems.length) {
    summaryParts.push(
      `${pinnedItems.length} important entr${pinnedItems.length === 1 ? "y is" : "ies are"} pinned to the top.`,
    );
  }
  if (latest?.created_at) {
    summaryParts.push(
      `Latest communication logged ${new Date(latest.created_at).toLocaleString()}.`,
    );
  }

  const nextActions = [
    unresolved.length
      ? "Review open follow-ups and assign a clear owner for each response."
      : "",
    overdueFollowups.length
      ? "Address overdue follow-ups first so the relationship history stays current."
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
    needs_response_count: needsResponseItems.length,
    pinned_count: pinnedItems.length,
    overdue_followups_count: overdueFollowups.length,
    counts_by_type: countsByType,
    counts_by_channel: countsByChannel,
    next_actions: nextActions,
    summary: summaryParts.join(" "),
  };
}

export function buildCommunicationSummary(rows: any[]) {
  return computeSummaryParts(rows, "All activity");
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
    follow_up_due_at: row?.follow_up_due_at || null,
    owner_id: row?.owner_id ? String(row.owner_id) : null,
    owner_name: row?.owner_name ? String(row.owner_name) : null,
    owner_email: row?.owner_email ? String(row.owner_email) : null,
    needs_response: Boolean(row?.needs_response),
    is_pinned: Boolean(row?.is_pinned),
    pinned_at: row?.pinned_at || null,
    pinned_by: row?.pinned_by ? String(row.pinned_by) : null,
    converted_from_id: row?.converted_from_id
      ? String(row.converted_from_id)
      : null,
    related_task_id: row?.related_task_id ? String(row.related_task_id) : null,
    related_note_id: row?.related_note_id ? String(row.related_note_id) : null,
    escalation_level: row?.escalation_level
      ? String(row.escalation_level)
      : null,
    risk_level: row?.risk_level ? String(row.risk_level) : null,
    created_by: row?.created_by ? String(row.created_by) : null,
    created_at: row?.created_at || null,
    updated_at: row?.updated_at || null,
  };
}

async function loadClientItems(clientId: string, limit = 250) {
  const cols = await getColumnSet("communications_timeline");
  const r = await query(
    `
    SELECT
      id, client_id, type, title, body, direction, channel, source, source_ref,
      status, priority, sentiment,
      ${selectCol(cols, "follow_up_due_at", "NULL::timestamptz")},
      ${selectCol(cols, "owner_id", "NULL::text")},
      ${selectCol(cols, "owner_name", "NULL::text")},
      ${selectCol(cols, "owner_email", "NULL::text")},
      ${selectCol(cols, "needs_response", "false::boolean")},
      ${selectCol(cols, "is_pinned", "false::boolean")},
      ${selectCol(cols, "pinned_at", "NULL::timestamptz")},
      ${selectCol(cols, "pinned_by", "NULL::text")},
      ${selectCol(cols, "converted_from_id", "NULL::text")},
      ${selectCol(cols, "related_task_id", "NULL::text")},
      ${selectCol(cols, "related_note_id", "NULL::text")},
      ${selectCol(cols, "escalation_level", "NULL::text")},
      ${selectCol(cols, "risk_level", "NULL::text")},
      created_by, created_at, updated_at
    FROM communications_timeline
    WHERE client_id = $1
    ORDER BY
      ${cols.has("is_pinned") ? "is_pinned DESC," : ""}
      ${cols.has("pinned_at") ? "COALESCE(pinned_at, created_at) DESC," : ""}
      created_at DESC
    LIMIT $2
    `,
    [clientId, limit],
  ).catch((e: any) => {
    if (isPgUndefinedTable(e)) return { rows: [] as any[] };
    throw e;
  });

  return (r.rows || []).map(rowToItem);
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

    const items = await loadClientItems(clientId, 250);
    const summary = buildCommunicationSummary(items);
    const now = Date.now();
    const dayMs = 24 * 60 * 60 * 1000;
    const last7 = items.filter((item) => {
      const t = item?.created_at ? new Date(item.created_at).getTime() : NaN;
      return Number.isFinite(t) && t >= now - 7 * dayMs;
    });
    const last30 = items.filter((item) => {
      const t = item?.created_at ? new Date(item.created_at).getTime() : NaN;
      return Number.isFinite(t) && t >= now - 30 * dayMs;
    });
    const unresolved = items.filter((item) => {
      const status = safeTrim(item?.status).toLowerCase();
      return Boolean(item?.needs_response) || UNRESOLVED_STATUSES.has(status);
    });

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
        needs_response_count: summary.needs_response_count,
        pinned_count: summary.pinned_count,
        overdue_followups_count: summary.overdue_followups_count,
      },
      next_actions: summary.next_actions,
      counts_by_type: summary.counts_by_type,
      counts_by_channel: summary.counts_by_channel,
      latest_at: summary.latest_at,
      windows: {
        last_7_days: computeSummaryParts(last7, "Last 7 days"),
        last_30_days: computeSummaryParts(last30, "Last 30 days"),
        unresolved: computeSummaryParts(unresolved, "Unresolved client issues"),
      },
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

    const items = await loadClientItems(clientId, limit);
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
    const cols = await getColumnSet("communications_timeline");

    const clientId = safeTrim(req.params.clientId);
    const type = normalizeType(req.body?.type || "note");
    const title = safeTrim(req.body?.title || "");
    const body = safeTrim(req.body?.body || "");
    const direction = safeTrim(req.body?.direction || "");
    const channel = safeTrim(req.body?.channel || "");
    const source = safeTrim(req.body?.source || "manual") || "manual";
    const sourceRef = safeTrim(
      req.body?.sourceRef || req.body?.source_ref || "",
    );
    const status = safeTrim(req.body?.status || "");
    const priority = safeTrim(req.body?.priority || "");
    const sentiment = safeTrim(req.body?.sentiment || "");
    const followUpDueAt = parseDateOrNull(
      req.body?.followUpDueAt || req.body?.follow_up_due_at,
    );
    const ownerId = safeTrim(req.body?.ownerId || req.body?.owner_id || "");
    const ownerName = safeTrim(
      req.body?.ownerName || req.body?.owner_name || "",
    );
    const ownerEmail = safeTrim(
      req.body?.ownerEmail || req.body?.owner_email || "",
    );
    const needsResponse = parseBoolean(
      req.body?.needsResponse ?? req.body?.needs_response,
      false,
    );
    const isPinned = parseBoolean(
      req.body?.isPinned ?? req.body?.is_pinned,
      false,
    );
    const pinnedAt = isPinned ? new Date().toISOString() : null;
    const convertedFromId = safeTrim(
      req.body?.convertedFromId || req.body?.converted_from_id || "",
    );
    const relatedTaskId = safeTrim(
      req.body?.relatedTaskId || req.body?.related_task_id || "",
    );
    const relatedNoteId = safeTrim(
      req.body?.relatedNoteId || req.body?.related_note_id || "",
    );
    const escalationLevel = safeTrim(
      req.body?.escalationLevel || req.body?.escalation_level || "",
    );
    const riskLevel = normalizeRiskLevel(
      req.body?.riskLevel || req.body?.risk_level,
    );

    if (!clientId || !body) {
      return res
        .status(400)
        .json({ ok: false, error: "clientId and body are required" });
    }

    const columns = [
      "client_id",
      "type",
      "title",
      "body",
      "direction",
      "channel",
      "source",
      "source_ref",
      "status",
      "priority",
      "sentiment",
    ];
    const values: any[] = [
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
    ];

    const maybeAdd = (column: string, value: any) => {
      if (cols.has(column)) {
        columns.push(column);
        values.push(value);
      }
    };

    maybeAdd("follow_up_due_at", followUpDueAt);
    maybeAdd("owner_id", ownerId || null);
    maybeAdd("owner_name", ownerName || null);
    maybeAdd("owner_email", ownerEmail || null);
    maybeAdd("needs_response", needsResponse);
    maybeAdd("is_pinned", isPinned);
    maybeAdd("pinned_at", pinnedAt);
    maybeAdd(
      "pinned_by",
      isPinned ? safeTrim(req.user?.id || "") || null : null,
    );
    maybeAdd("converted_from_id", convertedFromId || null);
    maybeAdd("related_task_id", relatedTaskId || null);
    maybeAdd("related_note_id", relatedNoteId || null);
    maybeAdd("escalation_level", escalationLevel || null);
    maybeAdd("risk_level", riskLevel);
    maybeAdd("created_by", safeTrim(req.user?.id || ""));

    const placeholders = columns.map((_, index) => `$${index + 1}`).join(",");
    const inserted = await query(
      `
      INSERT INTO communications_timeline (${columns.join(", ")})
      VALUES (${placeholders})
      RETURNING *
      `,
      values,
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
        follow_up_due_at: followUpDueAt,
        owner_name: ownerName || null,
        needs_response: needsResponse,
        is_pinned: isPinned,
        risk_level: riskLevel,
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
    const cols = await getColumnSet("communications_timeline");
    const id = safeTrim(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: "id required" });

    const existing = await query(
      `SELECT * FROM communications_timeline WHERE id = $1 LIMIT 1`,
      [id],
    );

    if (!existing.rows?.[0]) {
      return res
        .status(404)
        .json({ ok: false, error: "Communication entry not found" });
    }

    const prev = rowToItem(existing.rows[0]);
    const patch: Record<string, any> = {};
    const bodyObj = req.body || {};

    if (Object.prototype.hasOwnProperty.call(bodyObj, "type"))
      patch.type = normalizeType(bodyObj.type || prev.type);
    if (Object.prototype.hasOwnProperty.call(bodyObj, "title"))
      patch.title = safeTrim(bodyObj.title || "") || null;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "body"))
      patch.body = safeTrim(bodyObj.body || "") || prev.body;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "direction"))
      patch.direction = safeTrim(bodyObj.direction || "") || null;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "channel"))
      patch.channel = safeTrim(bodyObj.channel || "") || null;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "source"))
      patch.source = safeTrim(bodyObj.source || "") || null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "sourceRef") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "source_ref")
    )
      patch.source_ref =
        safeTrim(bodyObj.sourceRef || bodyObj.source_ref || "") || null;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "status"))
      patch.status = safeTrim(bodyObj.status || "") || null;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "priority"))
      patch.priority = safeTrim(bodyObj.priority || "") || null;
    if (Object.prototype.hasOwnProperty.call(bodyObj, "sentiment"))
      patch.sentiment = safeTrim(bodyObj.sentiment || "") || null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "followUpDueAt") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "follow_up_due_at")
    )
      patch.follow_up_due_at = parseDateOrNull(
        bodyObj.followUpDueAt || bodyObj.follow_up_due_at,
      );
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "ownerId") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "owner_id")
    )
      patch.owner_id =
        safeTrim(bodyObj.ownerId || bodyObj.owner_id || "") || null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "ownerName") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "owner_name")
    )
      patch.owner_name =
        safeTrim(bodyObj.ownerName || bodyObj.owner_name || "") || null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "ownerEmail") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "owner_email")
    )
      patch.owner_email =
        safeTrim(bodyObj.ownerEmail || bodyObj.owner_email || "") || null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "needsResponse") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "needs_response")
    )
      patch.needs_response = parseBoolean(
        bodyObj.needsResponse ?? bodyObj.needs_response,
        prev.needs_response,
      );
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "isPinned") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "is_pinned")
    ) {
      patch.is_pinned = parseBoolean(
        bodyObj.isPinned ?? bodyObj.is_pinned,
        prev.is_pinned,
      );
      patch.pinned_at = patch.is_pinned
        ? prev.pinned_at || new Date().toISOString()
        : null;
      patch.pinned_by = patch.is_pinned
        ? safeTrim(req.user?.id || "") || prev.pinned_by || null
        : null;
    }
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "convertedFromId") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "converted_from_id")
    )
      patch.converted_from_id =
        safeTrim(bodyObj.convertedFromId || bodyObj.converted_from_id || "") ||
        null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "relatedTaskId") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "related_task_id")
    )
      patch.related_task_id =
        safeTrim(bodyObj.relatedTaskId || bodyObj.related_task_id || "") ||
        null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "relatedNoteId") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "related_note_id")
    )
      patch.related_note_id =
        safeTrim(bodyObj.relatedNoteId || bodyObj.related_note_id || "") ||
        null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "escalationLevel") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "escalation_level")
    )
      patch.escalation_level =
        safeTrim(bodyObj.escalationLevel || bodyObj.escalation_level || "") ||
        null;
    if (
      Object.prototype.hasOwnProperty.call(bodyObj, "riskLevel") ||
      Object.prototype.hasOwnProperty.call(bodyObj, "risk_level")
    )
      patch.risk_level = normalizeRiskLevel(
        bodyObj.riskLevel || bodyObj.risk_level,
      );

    const filteredPatch = Object.fromEntries(
      Object.entries(patch).filter(([column]) => cols.has(column)),
    );
    if (!Object.keys(filteredPatch).length) {
      return res.json({ ok: true, item: prev });
    }

    const fields: string[] = [];
    const params: any[] = [id];
    let index = 2;
    for (const [column, value] of Object.entries(filteredPatch)) {
      fields.push(`${column} = $${index}`);
      params.push(value);
      index += 1;
    }
    fields.push(`updated_at = NOW()`);

    const updated = await query(
      `
      UPDATE communications_timeline
      SET ${fields.join(",\n        ")}
      WHERE id = $1
      RETURNING *
      `,
      params,
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
