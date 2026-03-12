// services/api/src/routes/ai.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";
import { clickupListTasks, hasClickUp } from "../integrations/clickup.js";

type DbRow = Record<string, any>;

type InsightSource = {
  type: string;
  label: string;
  confidence?: number;
  count?: number;
  available?: boolean;
};

type TaskItem = {
  id: string;
  title: string;
  status: string | null;
  priority: string | null;
  due_date: string | null;
  updated_at: string | null;
  source_table: string;
};

type TicketItem = {
  id: string;
  subject: string;
  status: string | null;
  priority: string | null;
  updated_at: string | null;
  source_table: string;
};

type ChatHistoryItem = {
  role?: string;
  text?: string;
  content?: string;
};

const router: ExpressRouter = Router();

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function toNum(v: any) {
  const n = Number(v ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function toIso(v: any) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function daysBetween(from: Date, value?: string | null) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return Math.ceil((d.getTime() - from.getTime()) / 86400000);
}

function safeLower(v: any) {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function money(value: any, currency = "USD") {
  const amount = toNum(value);
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currency || "USD",
      maximumFractionDigits: 2,
    }).format(amount);
  } catch {
    return `$${amount.toFixed(2)}`;
  }
}

function normalizeHistory(
  history: any,
): Array<{ role: "user" | "assistant"; text: string }> {
  if (!Array.isArray(history)) return [];

  return history
    .map((item: ChatHistoryItem) => {
      const rawRole = safeLower(item?.role);
      const role: "user" | "assistant" =
        rawRole === "ai" || rawRole === "assistant" ? "assistant" : "user";
      const text = String(item?.text ?? item?.content ?? "").trim();
      return { role, text };
    })
    .filter((item) => item.text)
    .slice(-8);
}

async function tableExists(tableName: string) {
  const r = await query<{ exists: boolean }>(
    `
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = $1
    ) AS exists
    `,
    [tableName],
  );
  return !!r.rows[0]?.exists;
}

async function columnsFor(tableName: string) {
  const r = await query<{ column_name: string }>(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
    ORDER BY ordinal_position ASC
    `,
    [tableName],
  );
  return new Set(r.rows.map((x) => x.column_name));
}

async function firstExistingTable(candidates: string[]) {
  for (const name of candidates) {
    if (await tableExists(name)) return name;
  }
  return null;
}

async function getClientSnapshot(clientId: string) {
  const local = await query<DbRow>(
    `
    SELECT
      c.id,
      c.name,
      c.status,
      c.whmcs_client_id,
      c.created_at,
      c.updated_at,
      w.company_name AS whmcs_company_name,
      w.status       AS whmcs_status,
      w.email        AS whmcs_email,
      w.currency     AS whmcs_currency,
      w.date_created AS whmcs_date_created,
      w.last_synced_at AS whmcs_last_synced_at
    FROM clients c
    LEFT JOIN whmcs_clients_cache w
      ON w.whmcs_client_id = c.whmcs_client_id
    WHERE c.id = $1
    LIMIT 1
    `,
    [clientId],
  );

  if (local.rows[0]) return local.rows[0];

  const match = /^whmcs-(\d+)$/i.exec(clientId);
  const whmcsId = match ? Number(match[1]) : null;
  if (!whmcsId) return null;

  const whmcsOnly = await query<DbRow>(
    `
    SELECT
      $1::text AS id,
      COALESCE(w.company_name, CONCAT('WHMCS #', w.whmcs_client_id::text)) AS name,
      COALESCE(NULLIF(TRIM(w.status), ''), 'active') AS status,
      w.whmcs_client_id,
      w.date_created AS created_at,
      w.last_synced_at AS updated_at,
      w.company_name AS whmcs_company_name,
      w.status       AS whmcs_status,
      w.email        AS whmcs_email,
      w.currency     AS whmcs_currency,
      w.date_created AS whmcs_date_created,
      w.last_synced_at AS whmcs_last_synced_at
    FROM whmcs_clients_cache w
    WHERE w.whmcs_client_id = $2
    LIMIT 1
    `,
    [clientId, whmcsId],
  );

  return whmcsOnly.rows[0] || null;
}

async function getBillingSummary(whmcsClientId: number | null) {
  if (!whmcsClientId) {
    return {
      mrr: 0,
      active_services: 0,
      suspended_services: 0,
      services_due_30d: 0,
      invoices_total: 0,
      open_invoices: 0,
      unpaid_invoices: 0,
      overdue_invoices: 0,
      paid_invoices: 0,
      paid_last_90d: 0,
      total_billed: 0,
      balance_due: 0,
      latest_invoice_due_date: null,
      next_renewal_date: null,
      renewal_value_30d: 0,
    };
  }

  const invoices = await query<DbRow>(
    `
    SELECT
      COUNT(*)::int AS invoices_total,
      COUNT(*) FILTER (WHERE LOWER(status) IN ('unpaid','payment pending','collections'))::int AS unpaid_invoices,
      COUNT(*) FILTER (WHERE COALESCE(balance, 0) > 0)::int AS open_invoices,
      COUNT(*) FILTER (
        WHERE COALESCE(balance, 0) > 0
          AND date_due IS NOT NULL
          AND date_due < NOW()
      )::int AS overdue_invoices,
      COUNT(*) FILTER (WHERE LOWER(status) = 'paid')::int AS paid_invoices,
      COUNT(*) FILTER (
        WHERE LOWER(status) = 'paid'
          AND date_paid >= NOW() - INTERVAL '90 days'
      )::int AS paid_last_90d,
      COALESCE(SUM(total), 0)::float AS total_billed,
      COALESCE(SUM(balance), 0)::float AS balance_due,
      MAX(date_due) FILTER (WHERE COALESCE(balance, 0) > 0) AS latest_invoice_due_date
    FROM whmcs_invoices_cache
    WHERE whmcs_client_id = $1
    `,
    [whmcsClientId],
  );

  const services = await query<DbRow>(
    `
    SELECT
      COUNT(*) FILTER (WHERE LOWER(status) IN ('active','completed'))::int AS active_services,
      COUNT(*) FILTER (WHERE LOWER(status) IN ('suspended','overdue','terminated','cancelled','fraud'))::int AS suspended_services,
      COUNT(*) FILTER (
        WHERE next_due_date IS NOT NULL
          AND next_due_date >= NOW()
          AND next_due_date < NOW() + INTERVAL '30 days'
      )::int AS services_due_30d,
      COALESCE(SUM(recurring_amount) FILTER (WHERE LOWER(status) = 'active'), 0)::float AS mrr,
      MIN(next_due_date) FILTER (
        WHERE next_due_date IS NOT NULL
          AND LOWER(status) IN ('active','completed','pending')
      ) AS next_renewal_date,
      COALESCE(SUM(recurring_amount) FILTER (
        WHERE next_due_date IS NOT NULL
          AND next_due_date >= NOW()
          AND next_due_date < NOW() + INTERVAL '30 days'
      ), 0)::float AS renewal_value_30d
    FROM whmcs_services_cache
    WHERE whmcs_client_id = $1
    `,
    [whmcsClientId],
  );

  return {
    ...Object.fromEntries(
      Object.entries(invoices.rows[0] || {}).map(([k, v]) => [k, v]),
    ),
    ...Object.fromEntries(
      Object.entries(services.rows[0] || {}).map(([k, v]) => [k, v]),
    ),
  } as DbRow;
}

async function ensureTasksTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id text NOT NULL,
      title text NOT NULL,
      description text,
      status text DEFAULT 'open',
      source text DEFAULT 'local',
      clickup_task_id text,
      created_at timestamptz DEFAULT now()
    )
  `);
}

function normalizeRemoteTask(task: any): TaskItem {
  const rawId =
    task?.id ??
    task?.task_id ??
    task?.gid ??
    task?.uuid ??
    `${task?.name || task?.title || "remote-task"}`;

  const rawTitle =
    task?.name ?? task?.title ?? task?.subject ?? "Untitled task";

  const rawStatus =
    typeof task?.status === "string"
      ? task.status
      : (task?.status?.status ?? task?.status?.type ?? task?.state ?? null);

  const rawPriority =
    typeof task?.priority === "string"
      ? task.priority
      : (task?.priority?.priority ?? task?.priority?.name ?? null);

  const rawDueDate =
    task?.due_date ??
    task?.dueDate ??
    task?.date_due ??
    task?.due_at ??
    task?.due_date_time ??
    null;

  const rawUpdatedAt =
    task?.date_updated ??
    task?.updated_at ??
    task?.updatedAt ??
    task?.date_closed ??
    task?.date_created ??
    null;

  return {
    id: String(rawId),
    title: String(rawTitle),
    status: rawStatus ? String(rawStatus) : null,
    priority: rawPriority ? String(rawPriority) : null,
    due_date: toIso(rawDueDate),
    updated_at: toIso(rawUpdatedAt),
    source_table: "clickup_remote",
  };
}

async function getTaskSummary(clientId: string) {
  await ensureTasksTable();

  const taskColumns = await columnsFor("tasks");
  const hasPriority = taskColumns.has("priority");
  const hasDueDate = taskColumns.has("due_date");
  const hasUpdatedAt = taskColumns.has("updated_at");
  const hasCreatedAt = taskColumns.has("created_at");
  const hasSource = taskColumns.has("source");

  const localResult = await query<DbRow>(
    `
    SELECT
      id::text AS id,
      title::text AS title,
      status::text AS status,
      ${hasPriority ? `priority::text` : `NULL::text`} AS priority,
      ${hasDueDate ? `due_date` : `NULL::timestamptz`} AS due_date,
      ${
        hasUpdatedAt
          ? `updated_at`
          : hasCreatedAt
            ? `created_at`
            : `NULL::timestamptz`
      } AS updated_at,
      ${hasSource ? `source::text` : `'local'::text`} AS source
    FROM tasks
    WHERE client_id = $1
    ORDER BY ${
      hasUpdatedAt ? `updated_at` : hasCreatedAt ? `created_at` : `id`
    } DESC NULLS LAST
    LIMIT 12
    `,
    [clientId],
  );

  const localItems: TaskItem[] = localResult.rows.map((row) => ({
    id: String(row.id),
    title: String(row.title || "Untitled task"),
    status: row.status ? String(row.status) : null,
    priority: row.priority ? String(row.priority) : null,
    due_date: toIso(row.due_date),
    updated_at: toIso(row.updated_at),
    source_table: row.source ? `tasks:${String(row.source)}` : "tasks:local",
  }));

  let remoteItems: TaskItem[] = [];
  let remoteAvailable = false;

  try {
    if (hasClickUp()) {
      const remote = await clickupListTasks({ tag: `client:${clientId}` });
      remoteItems = Array.isArray(remote)
        ? remote.map(normalizeRemoteTask)
        : [];
      remoteAvailable = true;
    }
  } catch {
    console.warn("[ai] ClickUp task fetch failed");
  }

  const mergedMap = new Map<string, TaskItem>();
  for (const item of [...remoteItems, ...localItems]) {
    const key =
      String(item.id || "") || `${item.title}:${item.updated_at || ""}`;
    if (!mergedMap.has(key)) mergedMap.set(key, item);
  }

  const items = [...mergedMap.values()]
    .sort((a, b) => {
      const ad = new Date(a.updated_at || a.due_date || 0).getTime();
      const bd = new Date(b.updated_at || b.due_date || 0).getTime();
      return bd - ad;
    })
    .slice(0, 12);

  const now = new Date();
  const normalized = items.map((x) => ({
    ...x,
    statusLower: safeLower(x.status),
  }));
  const isClosed = (s: string) =>
    [
      "done",
      "closed",
      "complete",
      "completed",
      "resolved",
      "cancelled",
    ].includes(s);

  const open = normalized.filter((x) => !isClosed(x.statusLower)).length;
  const inProgress = normalized.filter((x) =>
    [
      "in progress",
      "active",
      "working",
      "doing",
      "progress",
      "in_progress",
    ].includes(x.statusLower),
  ).length;
  const blocked = normalized.filter((x) =>
    ["blocked", "on hold", "stuck", "waiting", "pending external"].includes(
      x.statusLower,
    ),
  ).length;
  const overdue = normalized.filter((x) => {
    const dd = daysBetween(now, x.due_date);
    return dd !== null && dd < 0 && !isClosed(x.statusLower);
  }).length;

  return {
    available: localItems.length > 0 || remoteAvailable,
    tableName: remoteAvailable ? "tasks + clickup" : "tasks",
    total: items.length,
    open,
    overdue,
    in_progress: inProgress,
    blocked,
    items,
  };
}

async function getTicketSummary(
  clientId: string,
  whmcsClientId: number | null,
) {
  const ticketTable = await firstExistingTable([
    "whmcs_tickets_cache",
    "support_tickets",
    "tickets",
  ]);
  if (!ticketTable) {
    return {
      available: false,
      tableName: null,
      total: 0,
      open: 0,
      urgent: 0,
      stale: 0,
      items: [] as TicketItem[],
    };
  }

  const columns = await columnsFor(ticketTable);
  const idCol = columns.has("id")
    ? "id"
    : columns.has("ticket_id")
      ? "ticket_id"
      : null;
  const subjectCol = columns.has("subject")
    ? "subject"
    : columns.has("title")
      ? "title"
      : columns.has("name")
        ? "name"
        : null;
  const statusCol = columns.has("status") ? "status" : null;
  const priorityCol = columns.has("priority") ? "priority" : null;
  const updatedCol = columns.has("updated_at")
    ? "updated_at"
    : columns.has("date_updated")
      ? "date_updated"
      : columns.has("last_reply_at")
        ? "last_reply_at"
        : null;

  let where = "1=1";
  const params: any[] = [];

  if (whmcsClientId && columns.has("whmcs_client_id")) {
    params.push(whmcsClientId);
    where = `whmcs_client_id = $${params.length}`;
  } else if (columns.has("client_id")) {
    params.push(clientId);
    where = `client_id::text = $${params.length}`;
  } else {
    return {
      available: false,
      tableName: ticketTable,
      total: 0,
      open: 0,
      urgent: 0,
      stale: 0,
      items: [] as TicketItem[],
    };
  }

  if (!idCol || !subjectCol) {
    return {
      available: false,
      tableName: ticketTable,
      total: 0,
      open: 0,
      urgent: 0,
      stale: 0,
      items: [] as TicketItem[],
    };
  }

  const sql = `
    SELECT
      ${idCol}::text AS id,
      ${subjectCol}::text AS subject,
      ${statusCol ? `${statusCol}::text` : `NULL::text`} AS status,
      ${priorityCol ? `${priorityCol}::text` : `NULL::text`} AS priority,
      ${updatedCol ? updatedCol : `NULL::timestamptz`} AS updated_at
    FROM ${ticketTable}
    WHERE ${where}
    ORDER BY ${updatedCol || idCol} DESC NULLS LAST
    LIMIT 12
  `;

  const r = await query<DbRow>(sql, params);
  const items: TicketItem[] = r.rows.map((row) => ({
    id: String(row.id),
    subject: String(row.subject || "Untitled ticket"),
    status: row.status ? String(row.status) : null,
    priority: row.priority ? String(row.priority) : null,
    updated_at: toIso(row.updated_at),
    source_table: ticketTable,
  }));

  const now = new Date();
  const open = items.filter(
    (x) => !["closed", "resolved", "done"].includes(safeLower(x.status)),
  ).length;
  const urgent = items.filter((x) =>
    ["high", "urgent", "critical"].includes(safeLower(x.priority)),
  ).length;
  const stale = items.filter((x) => {
    const age = daysBetween(
      new Date(x.updated_at || now.toISOString()),
      now.toISOString(),
    );
    return (
      age !== null &&
      Math.abs(age) >= 7 &&
      !["closed", "resolved", "done"].includes(safeLower(x.status))
    );
  }).length;

  return {
    available: true,
    tableName: ticketTable,
    total: items.length,
    open,
    urgent,
    stale,
    items,
  };
}

function calculateRisk(
  clientStatus: string,
  billing: DbRow,
  tasks: Awaited<ReturnType<typeof getTaskSummary>>,
  tickets: Awaited<ReturnType<typeof getTicketSummary>>,
) {
  let score = 8;
  const reasons: string[] = [];

  const balanceDue = toNum(billing.balance_due);
  const overdueInvoices = toNum(billing.overdue_invoices);
  const unpaidInvoices = toNum(billing.unpaid_invoices);
  const suspendedServices = toNum(billing.suspended_services);
  const servicesDueSoon = toNum(billing.services_due_30d);

  if (
    ["inactive", "at_risk", "past_due", "suspended"].includes(
      safeLower(clientStatus),
    )
  ) {
    score += 18;
    reasons.push(`client status is ${clientStatus || "non-active"}`);
  }
  if (balanceDue > 0) {
    score += Math.min(25, Math.round(balanceDue / 100));
    reasons.push(`balance due is ${balanceDue.toFixed(2)}`);
  }
  if (overdueInvoices > 0) {
    score += overdueInvoices * 12;
    reasons.push(
      `${overdueInvoices} overdue invoice${overdueInvoices === 1 ? "" : "s"}`,
    );
  }
  if (unpaidInvoices >= 2) {
    score += 8;
    reasons.push(`${unpaidInvoices} unpaid invoices`);
  }
  if (suspendedServices > 0) {
    score += suspendedServices * 10;
    reasons.push(
      `${suspendedServices} suspended/cancelled service${suspendedServices === 1 ? "" : "s"}`,
    );
  }
  if (tasks.available && tasks.overdue > 0) {
    score += tasks.overdue * 6;
    reasons.push(
      `${tasks.overdue} overdue task${tasks.overdue === 1 ? "" : "s"}`,
    );
  }
  if (tasks.available && tasks.blocked > 0) {
    score += tasks.blocked * 7;
    reasons.push(
      `${tasks.blocked} blocked task${tasks.blocked === 1 ? "" : "s"}`,
    );
  }
  if (tickets.available && tickets.urgent > 0) {
    score += tickets.urgent * 9;
    reasons.push(
      `${tickets.urgent} urgent ticket${tickets.urgent === 1 ? "" : "s"}`,
    );
  }
  if (tickets.available && tickets.stale > 0) {
    score += tickets.stale * 5;
    reasons.push(
      `${tickets.stale} stale open ticket${tickets.stale === 1 ? "" : "s"}`,
    );
  }
  if (servicesDueSoon > 0 && toNum(billing.renewal_value_30d) > 0) {
    score += 4;
    reasons.push(
      `${servicesDueSoon} renewal${servicesDueSoon === 1 ? "" : "s"} due within 30 days`,
    );
  }

  score = Math.max(0, Math.min(100, score));

  let band = "healthy";
  if (score >= 65) band = "high";
  else if (score >= 40) band = "medium";

  return {
    score,
    band,
    reasons: reasons.slice(0, 6),
  };
}

function buildOpportunities(args: {
  clientStatus: string;
  billing: DbRow;
  tasks: Awaited<ReturnType<typeof getTaskSummary>>;
  tickets: Awaited<ReturnType<typeof getTicketSummary>>;
  risk: ReturnType<typeof calculateRisk>;
}) {
  const items: string[] = [];
  const mrr = toNum(args.billing.mrr);
  const activeServices = toNum(args.billing.active_services);
  const paid90 = toNum(args.billing.paid_last_90d);
  const totalBilled = toNum(args.billing.total_billed);
  const balanceDue = toNum(args.billing.balance_due);

  if (activeServices <= 1) {
    items.push(
      "The client has a narrow service footprint. This is a strong upsell opportunity for managed support, monitoring, backup, security, or recurring retainers.",
    );
  }
  if (mrr > 0 && mrr < 250) {
    items.push(
      "Recurring revenue is currently low. Consider packaging additional monthly services to increase account value and retention.",
    );
  }
  if (paid90 >= 2 && balanceDue === 0) {
    items.push(
      "Billing looks stable, which creates a good window for proactive expansion or quarterly business review outreach.",
    );
  }
  if (totalBilled > 0 && activeServices > 0 && args.risk.score < 40) {
    items.push(
      "This account appears operationally healthy enough for an upsell conversation around adjacent services or longer-term agreement renewal.",
    );
  }
  if (
    args.tasks.available &&
    args.tasks.open === 0 &&
    args.tickets.available &&
    args.tickets.open === 0
  ) {
    items.push(
      "There are no visible open tasks or tickets, which may be a good moment to propose strategic improvements rather than reactive support.",
    );
  }
  if (
    safeLower(args.clientStatus) === "active" &&
    toNum(args.billing.services_due_30d) > 0
  ) {
    items.push(
      "An upcoming renewal creates a natural opportunity to revisit scope, pricing, and bundled services before renewal is finalized.",
    );
  }

  return items.slice(0, 5);
}

function buildNextBestActions(context: {
  billing: DbRow;
  tasks: Awaited<ReturnType<typeof getTaskSummary>>;
  tickets: Awaited<ReturnType<typeof getTicketSummary>>;
  risk: ReturnType<typeof calculateRisk>;
}) {
  const actions: string[] = [];
  const balanceDue = toNum(context.billing.balance_due);
  const mrr = toNum(context.billing.mrr);
  const activeServices = toNum(context.billing.active_services);

  if (balanceDue > 0) {
    actions.push(
      `Follow up on outstanding balance of ${balanceDue.toFixed(2)} and confirm payment date.`,
    );
  }
  if (toNum(context.billing.overdue_invoices) > 0) {
    actions.push(
      "Escalate overdue invoices and review whether service continuity or grace period is at risk.",
    );
  }
  if (toNum(context.billing.services_due_30d) > 0) {
    actions.push(
      "Start renewal outreach for services due within 30 days and confirm scope, pricing, and term.",
    );
  }
  if (context.tasks.available && context.tasks.blocked > 0) {
    actions.push(
      "Unblock stalled work items and assign an owner with a due date for each blocker.",
    );
  }
  if (context.tasks.available && context.tasks.overdue > 0) {
    actions.push(
      "Review overdue tasks and reprioritize any client-facing deliverables this week.",
    );
  }
  if (context.tickets.available && context.tickets.urgent > 0) {
    actions.push(
      "Review urgent tickets first and provide the client with a same-day update.",
    );
  }
  if (context.tickets.available && context.tickets.stale > 0) {
    actions.push(
      "Re-engage stale support threads to reduce perceived silence and churn risk.",
    );
  }
  if (activeServices <= 1) {
    actions.push(
      "Review the current service footprint and identify at least one adjacent recurring service to propose.",
    );
  }
  if (mrr > 0 && mrr < 250 && context.risk.score < 50) {
    actions.push(
      "Prepare an upsell or bundled-service recommendation to increase monthly recurring revenue.",
    );
  }

  if (actions.length === 0) {
    actions.push(
      "Maintain regular client check-ins and confirm there are no hidden billing, delivery, or support issues.",
    );
  }

  return actions.slice(0, 6);
}

function buildQuestionHints(question: string) {
  const q = safeLower(question);
  return {
    asksBilling:
      /(billing|invoice|payment|balance|mrr|revenue|money|past due|overdue)/.test(
        q,
      ),
    asksRenewal: /(renew|renewal|expiration|next due|due date|forecast)/.test(
      q,
    ),
    asksTasks:
      /(task|project|deliverable|blocker|blocked|deadline|milestone)/.test(q),
    asksTickets: /(ticket|support|issue|incident|help desk|case)/.test(q),
    asksRisk: /(risk|at risk|churn|health|problem|concern)/.test(q),
    asksOpportunity:
      /(improve|improvement|recommend|recommendation|suggest|opportunity|upsell|cross sell|grow)/.test(
        q,
      ),
    asksExecutiveSummary:
      /(summary|overview|snapshot|what's going on|status)/.test(q),
  };
}

function buildFallbackReply(args: {
  clientLabel: string;
  clientStatus: string;
  currency?: string | null;
  billing: DbRow;
  tasks: Awaited<ReturnType<typeof getTaskSummary>>;
  tickets: Awaited<ReturnType<typeof getTicketSummary>>;
  risk: ReturnType<typeof calculateRisk>;
  question: string;
  nextActions: string[];
  opportunities: string[];
}) {
  const hints = buildQuestionHints(args.question);
  const nextRenewal = args.billing.next_renewal_date
    ? new Date(args.billing.next_renewal_date).toLocaleDateString()
    : "No upcoming renewal found";

  const taskLine = args.tasks.available
    ? `Tasks: ${args.tasks.open} open, ${args.tasks.overdue} overdue, ${args.tasks.blocked} blocked.`
    : `Tasks: no connected task dataset found.`;

  const ticketLine = args.tickets.available
    ? `Tickets: ${args.tickets.open} open, ${args.tickets.urgent} urgent, ${args.tickets.stale} stale.`
    : `Tickets: no connected ticket dataset found.`;

  const renewalPrediction =
    toNum(args.billing.renewal_value_30d) > 0
      ? `Renewal forecast: ${money(args.billing.renewal_value_30d, args.currency || "USD")} expected within 30 days, next renewal around ${nextRenewal}.`
      : `Renewal forecast: no billable renewal value found in the next 30 days.`;

  const reasons = args.risk.reasons.length
    ? args.risk.reasons.join(", ")
    : "no major risk indicators detected";

  const directAnswerParts: string[] = [];

  if (hints.asksRisk) {
    directAnswerParts.push(
      `Risk is currently ${args.risk.band} at ${args.risk.score}/100. The main drivers are ${reasons}.`,
    );
  }
  if (hints.asksBilling) {
    directAnswerParts.push(
      `Billing is showing ${money(args.billing.balance_due, args.currency || "USD")} outstanding, ${toNum(args.billing.open_invoices)} open invoice(s), and MRR of ${money(args.billing.mrr, args.currency || "USD")}.`,
    );
  }
  if (hints.asksRenewal) {
    directAnswerParts.push(renewalPrediction);
  }
  if (hints.asksTasks) {
    directAnswerParts.push(taskLine);
  }
  if (hints.asksTickets) {
    directAnswerParts.push(ticketLine);
  }
  if (hints.asksOpportunity) {
    directAnswerParts.push(
      args.opportunities[0] ||
        "The clearest improvement opportunity is to deepen the client relationship with a proactive review and identify one adjacent service that increases recurring value.",
    );
  }
  if (!directAnswerParts.length) {
    directAnswerParts.push(
      "Based on the current client snapshot, the highest-value focus right now is billing health, renewal readiness, service expansion opportunities, and proactive follow-up on any blocked work or urgent support items.",
    );
  }

  return [
    `## ${args.clientLabel} — Client 360 AI Summary`,
    `Status: ${args.clientStatus || "active"}`,
    `Risk score: ${args.risk.score}/100 (${args.risk.band}) based on ${reasons}.`,
    ``,
    `### Direct answer`,
    ...directAnswerParts,
    ``,
    `### Billing intelligence`,
    `MRR estimate: ${money(args.billing.mrr, args.currency || "USD")}`,
    `Outstanding balance: ${money(args.billing.balance_due, args.currency || "USD")}`,
    `Invoices: ${toNum(args.billing.open_invoices)} open, ${toNum(args.billing.overdue_invoices)} overdue, ${toNum(args.billing.paid_last_90d)} paid in the last 90 days.`,
    ``,
    `### Renewal prediction`,
    renewalPrediction,
    ``,
    `### Work and support`,
    taskLine,
    ticketLine,
    ``,
    `### Opportunities`,
    ...(args.opportunities.length
      ? args.opportunities.map((x, i) => `${i + 1}. ${x}`)
      : [
          "1. No strong expansion opportunity was detected from the currently connected data.",
        ]),
    ``,
    `### Recommended actions`,
    ...args.nextActions.map((x, i) => `${i + 1}. ${x}`),
  ]
    .filter(Boolean)
    .join("\n");
}

async function askAnthropic(input: {
  clientLabel: string;
  question: string;
  history: Array<{ role: "user" | "assistant"; text: string }>;
  context: any;
}) {
  const apiKey = env("ANTHROPIC_API_KEY");
  if (!apiKey) return null;

  const model = env("ANTHROPIC_MODEL", "claude-sonnet-4-0");
  const system = [
    "You are the AI Client Success Copilot for No Limits Media OS.",
    "Your role is to help account managers, operations staff, and leadership understand a client's health, billing posture, renewal readiness, operational risks, and growth opportunities.",
    "You can answer questions about billing, invoices, revenue, MRR, renewals, task blockers, support issues, churn risk, client health, executive summaries, recommendations, and upsell opportunities.",
    "Use only the provided JSON context and recent conversation history.",
    "If some data is unavailable, say so plainly and do not invent it.",
    "Always provide practical, business-focused answers with clear reasoning.",
    "When relevant, structure the answer with concise markdown sections such as Direct Answer, Client Health, Billing Intelligence, Renewal Forecast, Risks, Opportunities, and Recommended Actions.",
    "Prefer concise but useful answers, and directly answer the user's latest question before expanding.",
  ].join(" ");

  const historyText = input.history.length
    ? input.history
        .map(
          (item) =>
            `${item.role === "assistant" ? "Copilot" : "User"}: ${item.text}`,
        )
        .join("\n")
    : "No prior conversation history provided.";

  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model,
      max_tokens: 1400,
      system,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: [
                `Client: ${input.clientLabel}`,
                `Latest question: ${input.question || "Provide a complete Client 360 summary."}`,
                `Recent conversation history:`,
                historyText,
                `Context JSON:`,
                JSON.stringify(input.context, null, 2),
              ].join("\n\n"),
            },
          ],
        },
      ],
    }),
  });

  const json: any = await response.json().catch(() => null);

  if (!response.ok) {
    const message =
      json?.error?.message ||
      json?.error?.type ||
      `Anthropic HTTP ${response.status}`;
    throw new Error(message);
  }

  const text = Array.isArray(json?.content)
    ? json.content
        .filter((item: any) => item?.type === "text")
        .map((item: any) => item?.text || "")
        .join("\n")
        .trim()
    : "";

  return {
    text,
    requestId:
      response.headers.get("request-id") ||
      response.headers.get("anthropic-request-id") ||
      null,
    model: json?.model || model,
  };
}

async function buildClientIntelligence(args: {
  clientId: string;
  clientName?: string;
  status?: string;
}) {
  const snapshot = await getClientSnapshot(args.clientId);
  if (!snapshot) return null;

  const clientLabel =
    snapshot.name ||
    snapshot.whmcs_company_name ||
    args.clientName ||
    args.clientId;
  const clientStatus =
    snapshot.status || snapshot.whmcs_status || args.status || "active";
  const whmcsClientId = snapshot.whmcs_client_id
    ? Number(snapshot.whmcs_client_id)
    : null;
  const currency = snapshot.whmcs_currency || "USD";

  const [billing, tasks, tickets] = await Promise.all([
    getBillingSummary(whmcsClientId),
    getTaskSummary(args.clientId),
    getTicketSummary(args.clientId, whmcsClientId),
  ]);

  const risk = calculateRisk(clientStatus, billing, tasks, tickets);
  const healthScore = clamp(100 - risk.score, 0, 100);
  const opportunities = buildOpportunities({
    clientStatus,
    billing,
    tasks,
    tickets,
    risk,
  });
  const nextActions = buildNextBestActions({
    billing,
    tasks,
    tickets,
    risk,
  });

  const alerts: string[] = [];
  if (toNum(billing.overdue_invoices) > 0) {
    alerts.push(
      `${toNum(billing.overdue_invoices)} overdue invoice${toNum(billing.overdue_invoices) === 1 ? "" : "s"} require follow-up.`,
    );
  }
  if (toNum(billing.balance_due) > 0) {
    alerts.push(
      `Outstanding balance of ${money(billing.balance_due, currency)} is still open.`,
    );
  }
  if (toNum(billing.suspended_services) > 0) {
    alerts.push(
      `${toNum(billing.suspended_services)} suspended or cancelled service${toNum(billing.suspended_services) === 1 ? "" : "s"} detected.`,
    );
  }
  if (tasks.available && tasks.blocked > 0) {
    alerts.push(
      `${tasks.blocked} blocked task${tasks.blocked === 1 ? "" : "s"} may affect delivery.`,
    );
  }
  if (tickets.available && tickets.urgent > 0) {
    alerts.push(
      `${tickets.urgent} urgent ticket${tickets.urgent === 1 ? "" : "s"} need attention.`,
    );
  }
  if (toNum(billing.services_due_30d) > 0) {
    alerts.push(
      `${toNum(billing.services_due_30d)} renewal${toNum(billing.services_due_30d) === 1 ? "" : "s"} due within 30 days.`,
    );
  }
  if (!alerts.length) {
    alerts.push("No critical client alerts detected right now.");
  }

  const summaryLine =
    `${clientLabel} is currently in the ${risk.band} risk band with a score of ${risk.score}/100. ` +
    `MRR is ${money(billing.mrr, currency)}, open balance is ${money(billing.balance_due, currency)}, ` +
    `${toNum(billing.active_services)} active service${toNum(billing.active_services) === 1 ? "" : "s"} are connected, ` +
    `and ${toNum(billing.open_invoices)} invoice${toNum(billing.open_invoices) === 1 ? "" : "s"} remain open.`;

  return {
    snapshot,
    clientLabel,
    clientStatus,
    whmcsClientId,
    currency,
    billing,
    tasks,
    tickets,
    risk,
    healthScore,
    opportunities,
    nextActions,
    alerts,
    summaryLine,
  };
}

async function getGlobalTaskSignals() {
  try {
    await ensureTasksTable();
    const taskColumns = await columnsFor("tasks");
    const hasDueDate = taskColumns.has("due_date");
    const hasStatus = taskColumns.has("status");

    const r = await query<DbRow>(
      `
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (
          WHERE LOWER(COALESCE(${hasStatus ? "status" : "'open'"}, '')) IN
          ('blocked','on hold','stuck','waiting','pending external')
        )::int AS blocked,
        COUNT(*) FILTER (
          WHERE ${hasDueDate ? "due_date IS NOT NULL AND due_date < NOW()" : "FALSE"}
            AND LOWER(COALESCE(${hasStatus ? "status" : "'open'"}, '')) NOT IN
            ('done','closed','complete','completed','resolved','cancelled')
        )::int AS overdue,
        COUNT(*) FILTER (
          WHERE LOWER(COALESCE(${hasStatus ? "status" : "'open'"}, '')) NOT IN
          ('done','closed','complete','completed','resolved','cancelled')
        )::int AS open
      FROM tasks
      `,
    );

    return {
      total: toNum(r.rows[0]?.total),
      blocked: toNum(r.rows[0]?.blocked),
      overdue: toNum(r.rows[0]?.overdue),
      open: toNum(r.rows[0]?.open),
    };
  } catch {
    return { total: 0, blocked: 0, overdue: 0, open: 0 };
  }
}

async function getGlobalTicketSignals() {
  try {
    const ticketTable = await firstExistingTable([
      "whmcs_tickets_cache",
      "support_tickets",
      "tickets",
    ]);
    if (!ticketTable) return { total: 0, open: 0, urgent: 0, stale: 0 };

    const columns = await columnsFor(ticketTable);
    const statusCol = columns.has("status") ? "status" : "NULL::text";
    const priorityCol = columns.has("priority") ? "priority" : "NULL::text";
    const updatedCol = columns.has("updated_at")
      ? "updated_at"
      : columns.has("date_updated")
        ? "date_updated"
        : columns.has("last_reply_at")
          ? "last_reply_at"
          : "NULL::timestamptz";

    const r = await query<DbRow>(
      `
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (
          WHERE LOWER(COALESCE(${statusCol}, '')) NOT IN ('closed','resolved','done')
        )::int AS open,
        COUNT(*) FILTER (
          WHERE LOWER(COALESCE(${priorityCol}, '')) IN ('high','urgent','critical')
        )::int AS urgent,
        COUNT(*) FILTER (
          WHERE ${updatedCol} IS NOT NULL
            AND ${updatedCol} < NOW() - INTERVAL '7 days'
            AND LOWER(COALESCE(${statusCol}, '')) NOT IN ('closed','resolved','done')
        )::int AS stale
      FROM ${ticketTable}
      `,
    );

    return {
      total: toNum(r.rows[0]?.total),
      open: toNum(r.rows[0]?.open),
      urgent: toNum(r.rows[0]?.urgent),
      stale: toNum(r.rows[0]?.stale),
    };
  } catch {
    return { total: 0, open: 0, urgent: 0, stale: 0 };
  }
}

async function buildGlobalInsights() {
  const merged = await query<DbRow>(
    `
    WITH invoice_rollup AS (
      SELECT
        i.whmcs_client_id,
        COALESCE(SUM(
          CASE
            WHEN COALESCE(i.status, '') ILIKE 'Paid' THEN 0
            ELSE COALESCE(i.balance, i.total, 0)
          END
        ), 0)::float AS balance_due,
        COUNT(*) FILTER (
          WHERE COALESCE(i.status, '') ILIKE ANY (ARRAY['Unpaid', 'Draft', 'Overdue', 'Payment Pending'])
        )::int AS open_invoices,
        COUNT(*) FILTER (
          WHERE COALESCE(i.status, '') ILIKE ANY (ARRAY['Unpaid', 'Overdue', 'Payment Pending'])
            AND i.date_due IS NOT NULL
            AND i.date_due < NOW()
        )::int AS overdue_invoices
      FROM whmcs_invoices_cache i
      GROUP BY i.whmcs_client_id
    ),
    service_rollup AS (
      SELECT
        s.whmcs_client_id,
        COALESCE(SUM(
          CASE
            WHEN COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed']) THEN COALESCE(s.recurring_amount, 0)
            ELSE 0
          END
        ), 0)::float AS mrr,
        COUNT(*) FILTER (
          WHERE COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed'])
        )::int AS active_services,
        COUNT(*) FILTER (
          WHERE COALESCE(s.status, '') ILIKE ANY (ARRAY['Suspended', 'Overdue', 'Terminated', 'Cancelled', 'Fraud'])
        )::int AS suspended_services,
        COUNT(*) FILTER (
          WHERE s.next_due_date IS NOT NULL
            AND s.next_due_date >= NOW()
            AND s.next_due_date < NOW() + INTERVAL '30 days'
        )::int AS renewals_due_30d,
        COALESCE(SUM(CASE
          WHEN s.next_due_date IS NOT NULL
            AND s.next_due_date >= NOW()
            AND s.next_due_date < NOW() + INTERVAL '30 days'
          THEN COALESCE(s.recurring_amount, 0)
          ELSE 0
        END), 0)::float AS renewal_value_30d,
        MIN(s.next_due_date) FILTER (
          WHERE s.next_due_date IS NOT NULL
            AND COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed', 'Pending'])
        ) AS next_renewal_date
      FROM whmcs_services_cache s
      GROUP BY s.whmcs_client_id
    ),
    local_clients AS (
      SELECT
        c.id::text AS id,
        COALESCE(NULLIF(c.name, ''), w.company_name, c.id::text) AS name,
        COALESCE(NULLIF(c.status, ''), LOWER(w.status), 'active') AS status,
        c.whmcs_client_id,
        'local'::text AS source,
        COALESCE(ir.balance_due, 0)::float AS balance_due,
        COALESCE(ir.open_invoices, 0)::int AS open_invoices,
        COALESCE(ir.overdue_invoices, 0)::int AS overdue_invoices,
        COALESCE(sr.mrr, 0)::float AS mrr,
        COALESCE(sr.active_services, 0)::int AS active_services,
        COALESCE(sr.suspended_services, 0)::int AS suspended_services,
        COALESCE(sr.renewals_due_30d, 0)::int AS renewals_due_30d,
        COALESCE(sr.renewal_value_30d, 0)::float AS renewal_value_30d,
        sr.next_renewal_date,
        LOWER(COALESCE(NULLIF(c.name, ''), w.company_name, c.id::text)) AS sort_name
      FROM clients c
      LEFT JOIN whmcs_clients_cache w ON w.whmcs_client_id = c.whmcs_client_id
      LEFT JOIN invoice_rollup ir ON ir.whmcs_client_id = c.whmcs_client_id
      LEFT JOIN service_rollup sr ON sr.whmcs_client_id = c.whmcs_client_id
    ),
    whmcs_only_clients AS (
      SELECT
        CONCAT('whmcs-', w.whmcs_client_id::text) AS id,
        COALESCE(NULLIF(w.company_name, ''), CONCAT('WHMCS #', w.whmcs_client_id::text)) AS name,
        COALESCE(LOWER(NULLIF(w.status, '')), 'active') AS status,
        w.whmcs_client_id,
        'whmcs'::text AS source,
        COALESCE(ir.balance_due, 0)::float AS balance_due,
        COALESCE(ir.open_invoices, 0)::int AS open_invoices,
        COALESCE(ir.overdue_invoices, 0)::int AS overdue_invoices,
        COALESCE(sr.mrr, 0)::float AS mrr,
        COALESCE(sr.active_services, 0)::int AS active_services,
        COALESCE(sr.suspended_services, 0)::int AS suspended_services,
        COALESCE(sr.renewals_due_30d, 0)::int AS renewals_due_30d,
        COALESCE(sr.renewal_value_30d, 0)::float AS renewal_value_30d,
        sr.next_renewal_date,
        LOWER(COALESCE(NULLIF(w.company_name, ''), CONCAT('WHMCS #', w.whmcs_client_id::text))) AS sort_name
      FROM whmcs_clients_cache w
      LEFT JOIN invoice_rollup ir ON ir.whmcs_client_id = w.whmcs_client_id
      LEFT JOIN service_rollup sr ON sr.whmcs_client_id = w.whmcs_client_id
      WHERE NOT EXISTS (
        SELECT 1 FROM clients c WHERE c.whmcs_client_id = w.whmcs_client_id
      )
    ),
    merged_clients AS (
      SELECT * FROM local_clients
      UNION ALL
      SELECT * FROM whmcs_only_clients
    )
    SELECT
      id,
      name,
      status,
      whmcs_client_id,
      source,
      balance_due,
      open_invoices,
      overdue_invoices,
      mrr,
      active_services,
      suspended_services,
      renewals_due_30d,
      renewal_value_30d,
      next_renewal_date,
      sort_name
    FROM merged_clients
    ORDER BY sort_name ASC, id ASC
    `,
  );

  const rows = (merged.rows || []).map((row) => {
    let score = 8;
    const reasons: string[] = [];

    if (
      ["inactive", "at_risk", "past_due", "suspended"].includes(
        safeLower(row.status),
      )
    ) {
      score += 18;
      reasons.push(`status is ${row.status || "non-active"}`);
    }
    if (toNum(row.balance_due) > 0) {
      score += Math.min(25, Math.round(toNum(row.balance_due) / 100));
      reasons.push(`balance due ${money(row.balance_due)}`);
    }
    if (toNum(row.overdue_invoices) > 0) {
      score += toNum(row.overdue_invoices) * 12;
      reasons.push(
        `${toNum(row.overdue_invoices)} overdue invoice${toNum(row.overdue_invoices) === 1 ? "" : "s"}`,
      );
    }
    if (toNum(row.suspended_services) > 0) {
      score += toNum(row.suspended_services) * 10;
      reasons.push(
        `${toNum(row.suspended_services)} suspended service${toNum(row.suspended_services) === 1 ? "" : "s"}`,
      );
    }
    if (toNum(row.renewals_due_30d) > 0 && toNum(row.renewal_value_30d) > 0) {
      score += 4;
      reasons.push(
        `${toNum(row.renewals_due_30d)} renewal${toNum(row.renewals_due_30d) === 1 ? "" : "s"} due within 30 days`,
      );
    }

    score = clamp(score, 0, 100);
    const band = score >= 65 ? "high" : score >= 40 ? "medium" : "healthy";

    const opportunities: string[] = [];
    if (toNum(row.active_services) <= 1) {
      opportunities.push("Single-service growth opportunity");
    }
    if (toNum(row.mrr) > 0 && toNum(row.mrr) < 250) {
      opportunities.push("Low MRR expansion opportunity");
    }
    if (toNum(row.renewals_due_30d) > 0) {
      opportunities.push("Upcoming renewal expansion opportunity");
    }
    if (score < 40 && toNum(row.balance_due) === 0) {
      opportunities.push("Healthy account ready for proactive review");
    }

    return {
      ...row,
      risk_score: score,
      risk_band: band,
      reasons,
      opportunities,
      source_label: row.whmcs_client_id ? "WHMCS Synced" : "Local / Manual",
    };
  });

  const topRiskClients = [...rows]
    .sort(
      (a, b) =>
        b.risk_score - a.risk_score ||
        toNum(b.balance_due) - toNum(a.balance_due),
    )
    .slice(0, 6)
    .map((row) => ({
      id: String(row.id),
      name: String(row.name),
      status: String(row.status || "active"),
      source_label: row.source_label,
      risk_score: row.risk_score,
      risk_band: row.risk_band,
      reasons: row.reasons.slice(0, 3),
      balance_due: toNum(row.balance_due),
      open_invoices: toNum(row.open_invoices),
      next_renewal_date: toIso(row.next_renewal_date),
    }));

  const renewalsNext = rows
    .filter((row) => toNum(row.renewals_due_30d) > 0)
    .sort((a, b) => {
      const ad = a.next_renewal_date
        ? new Date(a.next_renewal_date).getTime()
        : Number.MAX_SAFE_INTEGER;
      const bd = b.next_renewal_date
        ? new Date(b.next_renewal_date).getTime()
        : Number.MAX_SAFE_INTEGER;
      return ad - bd;
    })
    .slice(0, 6)
    .map((row) => ({
      id: String(row.id),
      name: String(row.name),
      next_renewal_date: toIso(row.next_renewal_date),
      renewal_value_30d: toNum(row.renewal_value_30d),
      renewals_due_30d: toNum(row.renewals_due_30d),
    }));

  const lowMrrCount = rows.filter(
    (row) => toNum(row.mrr) > 0 && toNum(row.mrr) < 250,
  ).length;
  const singleServiceCount = rows.filter(
    (row) => toNum(row.active_services) <= 1,
  ).length;
  const suspendedServiceClientCount = rows.filter(
    (row) => toNum(row.suspended_services) > 0,
  ).length;
  const healthyExpansionCount = rows.filter(
    (row) => row.risk_score < 40 && toNum(row.balance_due) === 0,
  ).length;

  const taskSignals = await getGlobalTaskSignals();
  const ticketSignals = await getGlobalTicketSignals();

  const alerts: string[] = [];
  if (rows.some((row) => row.risk_score >= 65)) {
    alerts.push(
      `${rows.filter((row) => row.risk_score >= 65).length} client accounts are in the high-risk band.`,
    );
  }
  if (taskSignals.blocked > 0) {
    alerts.push(
      `${taskSignals.blocked} blocked task${taskSignals.blocked === 1 ? "" : "s"} need review.`,
    );
  }
  if (ticketSignals.urgent > 0) {
    alerts.push(
      `${ticketSignals.urgent} urgent ticket${ticketSignals.urgent === 1 ? "" : "s"} are still open.`,
    );
  }
  if (!alerts.length) {
    alerts.push("No major global AI alerts detected right now.");
  }

  return {
    generated_at: new Date().toISOString(),
    overview: {
      total_clients: rows.length,
      whmcs_synced: rows.filter((row) => !!row.whmcs_client_id).length,
      healthy_clients: rows.filter((row) => row.risk_score < 40).length,
      medium_risk_clients: rows.filter(
        (row) => row.risk_score >= 40 && row.risk_score < 65,
      ).length,
      high_risk_clients: rows.filter((row) => row.risk_score >= 65).length,
      total_balance_due: rows.reduce(
        (sum, row) => sum + toNum(row.balance_due),
        0,
      ),
      renewal_value_30d: rows.reduce(
        (sum, row) => sum + toNum(row.renewal_value_30d),
        0,
      ),
      renewals_due_30d: rows.reduce(
        (sum, row) => sum + toNum(row.renewals_due_30d),
        0,
      ),
    },
    top_risk_clients: topRiskClients,
    opportunities: [
      {
        key: "single_service",
        title: "Single-service clients",
        count: singleServiceCount,
        description:
          "Accounts with a narrow service footprint that may be ready for adjacent recurring services.",
      },
      {
        key: "low_mrr",
        title: "Low-MRR expansion",
        count: lowMrrCount,
        description:
          "Accounts with recurring revenue below $250 that may benefit from bundled service recommendations.",
      },
      {
        key: "suspended_service_recovery",
        title: "Suspended service recovery",
        count: suspendedServiceClientCount,
        description:
          "Accounts with suspended or cancelled services that may need recovery or retention outreach.",
      },
      {
        key: "healthy_review",
        title: "Healthy review targets",
        count: healthyExpansionCount,
        description:
          "Operationally healthy accounts with room for proactive quarterly review and expansion.",
      },
    ],
    renewals: {
      upcoming_count: rows.reduce(
        (sum, row) => sum + toNum(row.renewals_due_30d),
        0,
      ),
      total_value_30d: rows.reduce(
        (sum, row) => sum + toNum(row.renewal_value_30d),
        0,
      ),
      next: renewalsNext,
    },
    operations: {
      open_tasks: taskSignals.open,
      blocked_tasks: taskSignals.blocked,
      overdue_tasks: taskSignals.overdue,
      open_tickets: ticketSignals.open,
      urgent_tickets: ticketSignals.urgent,
      stale_tickets: ticketSignals.stale,
    },
    alerts,
  };
}

router.post("/client-summary", async (req, res) => {
  const clientId = String(req.body?.clientId || "").trim();
  const clientName = String(req.body?.clientName || "").trim();
  const status = String(req.body?.status || "").trim();
  const question = String(req.body?.question || "").trim();
  const history = normalizeHistory(req.body?.history);

  if (!clientId) {
    return res.status(400).json({ ok: false, error: "clientId is required" });
  }

  try {
    const snapshot = await getClientSnapshot(clientId);
    if (!snapshot) {
      return res.status(404).json({ ok: false, error: "Client not found" });
    }

    const clientLabel =
      snapshot.name || snapshot.whmcs_company_name || clientName || clientId;
    const clientStatus =
      snapshot.status || snapshot.whmcs_status || status || "active";
    const whmcsClientId = snapshot.whmcs_client_id
      ? Number(snapshot.whmcs_client_id)
      : null;
    const currency = String(snapshot.whmcs_currency || "USD").trim() || "USD";

    const [billing, tasks, tickets] = await Promise.all([
      getBillingSummary(whmcsClientId),
      getTaskSummary(clientId),
      getTicketSummary(clientId, whmcsClientId),
    ]);

    const risk = calculateRisk(clientStatus, billing, tasks, tickets);
    const opportunities = buildOpportunities({
      clientStatus,
      billing,
      tasks,
      tickets,
      risk,
    });
    const nextActions = buildNextBestActions({ billing, tasks, tickets, risk });

    const context = {
      generated_at: new Date().toISOString(),
      client: {
        id: snapshot.id,
        name: clientLabel,
        status: clientStatus,
        whmcs_client_id: whmcsClientId,
        email: snapshot.whmcs_email || null,
        currency,
        created_at: toIso(snapshot.created_at),
        updated_at: toIso(snapshot.updated_at),
        whmcs_last_synced_at: toIso(snapshot.whmcs_last_synced_at),
      },
      billing: {
        mrr: toNum(billing.mrr),
        active_services: toNum(billing.active_services),
        suspended_services: toNum(billing.suspended_services),
        services_due_30d: toNum(billing.services_due_30d),
        invoices_total: toNum(billing.invoices_total),
        open_invoices: toNum(billing.open_invoices),
        unpaid_invoices: toNum(billing.unpaid_invoices),
        overdue_invoices: toNum(billing.overdue_invoices),
        paid_invoices: toNum(billing.paid_invoices),
        paid_last_90d: toNum(billing.paid_last_90d),
        total_billed: toNum(billing.total_billed),
        balance_due: toNum(billing.balance_due),
        latest_invoice_due_date: toIso(billing.latest_invoice_due_date),
      },
      renewal_prediction: {
        next_renewal_date: toIso(billing.next_renewal_date),
        renewal_value_30d: toNum(billing.renewal_value_30d),
      },
      tasks: {
        available: tasks.available,
        source_table: tasks.tableName,
        total: tasks.total,
        open: tasks.open,
        overdue: tasks.overdue,
        in_progress: tasks.in_progress,
        blocked: tasks.blocked,
        items: tasks.items,
      },
      tickets: {
        available: tickets.available,
        source_table: tickets.tableName,
        total: tickets.total,
        open: tickets.open,
        urgent: tickets.urgent,
        stale: tickets.stale,
        items: tickets.items,
      },
      risk,
      opportunities,
      next_best_actions: nextActions,
      conversation_history: history,
    };

    let reply = "";
    let aiMeta: { model?: string | null; requestId?: string | null } = {};

    try {
      const provider = env("AI_PROVIDER", "anthropic").toLowerCase();

      if (provider === "anthropic") {
        const ai = await askAnthropic({
          clientLabel,
          question: question || "Provide a complete Client 360 summary.",
          history,
          context,
        });

        if (ai?.text) {
          reply = ai.text;
          aiMeta = { model: ai.model, requestId: ai.requestId };
        }
      }
    } catch (err: any) {
      console.error(
        "[ai] Anthropic call failed, using fallback:",
        err?.message || err,
      );
    }

    if (!reply) {
      reply = buildFallbackReply({
        clientLabel,
        clientStatus,
        currency,
        billing,
        tasks,
        tickets,
        risk,
        question,
        nextActions,
        opportunities,
      });
    }

    const usingAnthropic =
      env("AI_PROVIDER", "anthropic").toLowerCase() === "anthropic" &&
      !!env("ANTHROPIC_API_KEY");

    const healthScore = clamp(100 - risk.score, 0, 100);

    const sources: InsightSource[] = [
      {
        type: "db",
        label: "Client profile + WHMCS client cache",
        confidence: 0.95,
        available: true,
      },
      {
        type: "db",
        label: "WHMCS invoices cache",
        confidence: 0.93,
        count: toNum(billing.invoices_total),
        available: whmcsClientId !== null,
      },
      {
        type: "db",
        label: "WHMCS services cache",
        confidence: 0.93,
        count:
          toNum(billing.active_services) + toNum(billing.suspended_services),
        available: whmcsClientId !== null,
      },
      {
        type: "db",
        label: tasks.available
          ? `Tasks from ${tasks.tableName}`
          : "Tasks not connected",
        confidence: tasks.available ? 0.82 : 0.2,
        count: tasks.total,
        available: tasks.available,
      },
      {
        type: "db",
        label: tickets.available
          ? `Tickets from ${tickets.tableName}`
          : "Tickets not connected",
        confidence: tickets.available ? 0.82 : 0.2,
        count: tickets.total,
        available: tickets.available,
      },
      {
        type: usingAnthropic ? "anthropic" : "fallback",
        label: usingAnthropic
          ? "Anthropic Claude Messages API"
          : "Rule-based fallback summary",
        confidence: usingAnthropic ? 0.89 : 0.74,
        available: true,
      },
    ];

    return res.json({
      ok: true,
      clientId,
      reply,
      sources,
      meta: {
        feature_set: [
          "billing_intelligence",
          "renewal_prediction",
          "risk_scoring",
          "ticket_summarization",
          "next_best_action_engine",
          "opportunity_detection",
          "conversation_memory",
          "executive_copilot_answers",
        ],
        health_score: healthScore,
        risk_score: risk.score,
        risk_band: risk.band,
        opportunity_count: opportunities.length,
        model: aiMeta.model || null,
        anthropic_request_id: aiMeta.requestId || null,
      },
      context_preview: {
        client_name: clientLabel,
        status: clientStatus,
        currency,
        mrr: toNum(billing.mrr),
        balance_due: toNum(billing.balance_due),
        open_invoices: toNum(billing.open_invoices),
        active_services: toNum(billing.active_services),
        open_tasks: tasks.open,
        urgent_tickets: tickets.urgent,
        next_renewal_date: toIso(billing.next_renewal_date),
      },
    });
  } catch (e: any) {
    console.error("[ai] client-summary error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "AI summary failed" });
  }
});

router.get("/client-insights/:clientId", async (req, res) => {
  const clientId = String(req.params?.clientId || "").trim();
  if (!clientId) {
    return res.status(400).json({ ok: false, error: "clientId is required" });
  }

  try {
    const intelligence = await buildClientIntelligence({ clientId });
    if (!intelligence) {
      return res.status(404).json({ ok: false, error: "Client not found" });
    }

    return res.json({
      ok: true,
      clientId,
      generated_at: new Date().toISOString(),
      client: {
        id: intelligence.snapshot.id,
        name: intelligence.clientLabel,
        status: intelligence.clientStatus,
        whmcs_client_id: intelligence.whmcsClientId,
        email: intelligence.snapshot.whmcs_email || null,
        currency: intelligence.currency,
      },
      summary: intelligence.summaryLine,
      metrics: {
        health_score: intelligence.healthScore,
        risk_score: intelligence.risk.score,
        risk_band: intelligence.risk.band,
        mrr: toNum(intelligence.billing.mrr),
        balance_due: toNum(intelligence.billing.balance_due),
        open_invoices: toNum(intelligence.billing.open_invoices),
        overdue_invoices: toNum(intelligence.billing.overdue_invoices),
        active_services: toNum(intelligence.billing.active_services),
        renewals_due_30d: toNum(intelligence.billing.services_due_30d),
        renewal_value_30d: toNum(intelligence.billing.renewal_value_30d),
        blocked_tasks: intelligence.tasks.blocked,
        urgent_tickets: intelligence.tickets.urgent,
      },
      alerts: intelligence.alerts,
      opportunities: intelligence.opportunities,
      next_best_actions: intelligence.nextActions,
      sources: [
        "Client profile + WHMCS client cache",
        "WHMCS invoices cache",
        "WHMCS services cache",
        intelligence.tasks.available
          ? `Tasks from ${intelligence.tasks.tableName}`
          : "Tasks unavailable",
        intelligence.tickets.available
          ? `Tickets from ${intelligence.tickets.tableName}`
          : "Tickets unavailable",
      ],
    });
  } catch (e: any) {
    console.error("[ai] client-insights error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to build client insights",
    });
  }
});

router.get("/global-insights", async (_req, res) => {
  try {
    const payload = await buildGlobalInsights();
    return res.json({ ok: true, ...payload });
  } catch (e: any) {
    console.error("[ai] global-insights error", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to build global insights",
    });
  }
});

export default router;
