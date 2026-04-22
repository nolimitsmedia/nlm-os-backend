// services/api/src/routes/tasks.tx
import { Router } from "express";
import multer from "multer";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import {
  buildTaskAttachmentStoragePath,
  deleteFromBunny,
  uploadBufferToBunny,
} from "../integrations/bunny.js";
import {
  clickupCreateTask,
  clickupListTasks,
  clickupUploadAttachment,
  deleteClickUpTask,
  hasClickUp,
  listClickUpAssignees,
  updateClickUpTask,
  updateClickUpTaskStatus,
  buildClickUpTaskUrl,
} from "../integrations/clickup.js";
import { isSharePointConfigured } from "../integrations/sharepoint.js";
import {
  sendTaskShareEmail,
  sendTaskAssignedEmail,
} from "../services/email.js";
import { createNotification } from "./notifications.js";

const router = Router();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 25 * 1024 * 1024,
    files: 10,
  },
});

type TasksSchemaInfo = {
  hasStatus: boolean;
  hasSource: boolean;
  hasClickupTaskId: boolean;
  hasCreatedAt: boolean;
  hasAssignee: boolean;
  hasAssigneeIds: boolean;
  hasDueDate: boolean;
  hasSopId: boolean;
  hasSopTitle: boolean;
  hasSopUrl: boolean;
  hasBillingDependency: boolean;
  hasWorkflowState: boolean;
  hasPriority: boolean;
  hasChecklist: boolean;
  hasTemplateKey: boolean;
  hasRecurringRule: boolean;
  hasSlaDueAt: boolean;
  hasDependencyIds: boolean;
  hasBillingAction: boolean;
  hasInvoiceIssueStatus: boolean;
  hasAutomationSource: boolean;
};

type TaskTemplateItem = {
  label: string;
  done?: boolean;
  required?: boolean;
};

type TaskTemplateRecord = {
  key: string;
  label: string;
  description: string;
  workflow_state?: string;
  checklist: TaskTemplateItem[];
  recurring_rule?: Record<string, any> | null;
  billing_action?: Record<string, any> | null;
};

type AttachmentMirrorResult = {
  fileName: string;
  mirrored: boolean;
  taskId?: string | null;
  error?: string | null;
  status?: number | null;
};

const TASK_TEMPLATE_LIBRARY: TaskTemplateRecord[] = [
  {
    key: "client_onboarding",
    label: "Client Onboarding",
    description: "Standard onboarding task package for a new client.",
    workflow_state: "intake",
    checklist: [
      { label: "Confirm scope and owner", required: true },
      { label: "Create communication timeline entry", required: true },
      { label: "Link SOP or onboarding docs", required: true },
      { label: "Schedule kickoff / follow-up" },
    ],
  },
  {
    key: "billing_followup",
    label: "Billing Follow-up",
    description: "Internal billing review and follow-up workflow.",
    workflow_state: "waiting_on_client",
    checklist: [
      { label: "Mark invoice issue for review", required: true },
      { label: "Assign billing owner", required: true },
      { label: "Request billing follow-up", required: true },
      { label: "Track exception / suspension recommendation" },
    ],
    billing_action: { type: "follow_up" },
  },
  {
    key: "recurring_health_check",
    label: "Recurring Health Check",
    description: "Repeatable client health review workflow.",
    workflow_state: "active",
    checklist: [
      { label: "Review AI client summary", required: true },
      { label: "Check overdue tasks and tickets", required: true },
      { label: "Review communications / follow-ups", required: true },
      { label: "Create next action tasks" },
    ],
    recurring_rule: { type: "monthly", interval: 1 },
  },
];

const WORKFLOW_STATES = [
  "intake",
  "todo",
  "active",
  "in_progress",
  "blocked",
  "waiting_on_client",
  "qa_review",
  "completed",
  "cancelled",
];

function buildTaskAppLink(task: any) {
  const base =
    String(process.env.APP_URL || process.env.FRONTEND_URL || "").trim() || "";
  const clientId = String(task?.client_id || "").trim();
  const taskId = String(task?.id || task?.clickup_task_id || "").trim();
  if (!clientId || !taskId) return "";
  const path = `/clients/${encodeURIComponent(clientId)}/tasks?task=${encodeURIComponent(taskId)}`;
  return base ? `${base.replace(/\/$/, "")}${path}` : path;
}

function normalizeEmail(value: any) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function extractEmailsFromAssigneeLabel(value: any) {
  const raw = String(value || "");
  const matches = raw.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi);
  return Array.from(
    new Set(
      (matches || []).map((item) => normalizeEmail(item)).filter(Boolean),
    ),
  );
}

async function findAdminsByEmails(emails: string[]) {
  const unique = Array.from(
    new Set((emails || []).map((item) => normalizeEmail(item)).filter(Boolean)),
  );
  if (!unique.length) return [];
  const result = await query(
    `
    SELECT id, email, name, role
    FROM public.admins
    WHERE LOWER(TRIM(email)) = ANY($1::text[])
    `,
    [unique],
  ).catch(() => ({ rows: [] }));
  return Array.isArray((result as any)?.rows) ? (result as any).rows : [];
}

async function resolveAssignmentTargets(args: {
  assigneeIds?: string[];
  assigneeLabel?: string;
  assigneeEmail?: string;
}) {
  const ids = Array.isArray(args.assigneeIds)
    ? args.assigneeIds
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    : [];
  let clickupAssignees: any[] = [];
  if (ids.length && hasClickUp()) {
    try {
      const members = await listClickUpAssignees({});
      clickupAssignees = Array.isArray(members) ? members : [];
    } catch {
      clickupAssignees = [];
    }
  }
  const matchedMembers = ids.length
    ? clickupAssignees.filter((member: any) =>
        ids.includes(String(member?.id || "").trim()),
      )
    : [];
  const fallbackEmails = extractEmailsFromAssigneeLabel(args.assigneeLabel);
  const directAssigneeEmail = normalizeEmail(args.assigneeEmail);
  const emailPool = Array.from(
    new Set(
      [
        ...matchedMembers.map((member: any) => normalizeEmail(member?.email)),
        ...fallbackEmails,
        directAssigneeEmail,
      ].filter(Boolean),
    ),
  );
  const admins = await findAdminsByEmails(emailPool);
  const adminByEmail = new Map(
    admins.map((admin: any) => [normalizeEmail(admin.email), admin]),
  );
  const targets = new Map<
    string,
    { email: string; name?: string; adminId?: string | null }
  >();
  for (const member of matchedMembers) {
    const email = normalizeEmail(member?.email);
    if (!email) continue;
    const admin = adminByEmail.get(email);
    targets.set(email, {
      email,
      name: String(
        member?.name || member?.username || admin?.name || email,
      ).trim(),
      adminId: admin?.id ? String(admin.id) : null,
    });
  }
  for (const email of fallbackEmails) {
    if (targets.has(email)) continue;
    const admin = adminByEmail.get(email);
    targets.set(email, {
      email,
      name: String(admin?.name || email).trim(),
      adminId: admin?.id ? String(admin.id) : null,
    });
  }
  if (directAssigneeEmail && !targets.has(directAssigneeEmail)) {
    const admin = adminByEmail.get(directAssigneeEmail);
    targets.set(directAssigneeEmail, {
      email: directAssigneeEmail,
      name: String(admin?.name || directAssigneeEmail).trim(),
      adminId: admin?.id ? String(admin.id) : null,
    });
  }
  return Array.from(targets.values());
}

async function notifyTaskAssignees(args: {
  task: any;
  clientName: string;
  actorName?: string;
  assigneeIds?: string[];
  assigneeLabel?: string;
  assigneeEmail?: string;
  reason?: "created" | "updated" | "assigned";
}) {
  const task = args.task || {};
  const shareUrl = buildTaskAppLink(task);
  const clickupUrl =
    String(task?.url || "").trim() ||
    buildClickUpTaskUrl(String(task?.clickup_task_id || ""));
  const taskTitle = String(task?.title || task?.name || "Untitled Task").trim();
  const targets = await resolveAssignmentTargets({
    assigneeIds: args.assigneeIds,
    assigneeLabel: args.assigneeLabel,
    assigneeEmail: args.assigneeEmail,
  });
  if (!targets.length) return;
  for (const target of targets) {
    if (target.adminId) {
      await createNotification({
        userId: target.adminId,
        kind: "task_assignment",
        title: `Task assigned: ${taskTitle}`,
        body: args.clientName
          ? `Client: ${args.clientName}`
          : "A task was assigned to you.",
        actionUrl: shareUrl || null,
        actionLabel: clickupUrl ? "Open task" : "Open task",
        meta: {
          task_id: String(task?.id || ""),
          clickup_task_id: String(task?.clickup_task_id || ""),
          client_id: String(task?.client_id || ""),
          clickup_url: clickupUrl || null,
          reason: args.reason || "assigned",
        },
      }).catch(() => null);
    }
    await sendTaskAssignedEmail({
      to: target.email,
      assigneeName: target.name,
      taskTitle,
      clientName: args.clientName,
      assignedByName: args.actorName,
      shareUrl: shareUrl || clickupUrl || "",
      clickupUrl: clickupUrl || null,
    }).catch(() => null);
  }
}

function normalizeWorkflowState(value: any) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) return "active";
  if (["in progress", "in-progress"].includes(normalized)) return "in_progress";
  if (["waiting on client", "waiting-on-client"].includes(normalized))
    return "waiting_on_client";
  if (["qa", "review"].includes(normalized)) return "qa_review";
  if (["done", "closed", "complete"].includes(normalized)) return "completed";
  return WORKFLOW_STATES.includes(normalized) ? normalized : "active";
}

function normalizePriority(value: any) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (["urgent", "critical"].includes(normalized)) return "high";
  if (!normalized) return "medium";
  return normalized;
}

function normalizeJsonArray(value: any) {
  if (Array.isArray(value)) return value;
  if (typeof value === "string" && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function normalizeJsonObject(value: any) {
  if (value && typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value === "string" && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed
        : null;
    } catch {
      return null;
    }
  }
  return null;
}

function buildChecklistFromTemplate(templateKey: string, incoming: any) {
  const incomingArray = normalizeJsonArray(incoming);
  if (incomingArray.length) return incomingArray;
  const template = TASK_TEMPLATE_LIBRARY.find(
    (item) => item.key === templateKey,
  );
  return template
    ? template.checklist.map((item) => ({ ...item, done: false }))
    : [];
}

function summarizeRecurringRule(rule: any) {
  const recurring = normalizeJsonObject(rule);
  if (!recurring) return "";
  const type =
    String(recurring.type || recurring.frequency || "").trim() || "custom";
  const interval = Number(recurring.interval || 1) || 1;
  return interval > 1 ? `Every ${interval} ${type}` : `Every ${type}`;
}

function parseDependencyIds(value: any) {
  return normalizeJsonArray(value)
    .map((item: any) => String(item || "").trim())
    .filter(Boolean);
}

function getTemplateRecord(templateKey: string) {
  return TASK_TEMPLATE_LIBRARY.find((item) => item.key === templateKey) || null;
}

async function getTasksSchemaInfo(): Promise<TasksSchemaInfo> {
  const r = await query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'tasks'
    `,
  );

  const cols = new Set(
    (r.rows || []).map((row: any) => String(row.column_name || "").trim()),
  );

  return {
    hasStatus: cols.has("status"),
    hasSource: cols.has("source"),
    hasClickupTaskId: cols.has("clickup_task_id"),
    hasCreatedAt: cols.has("created_at"),
    hasAssignee: cols.has("assignee"),
    hasAssigneeIds: cols.has("assignee_ids"),
    hasDueDate: cols.has("due_date"),
    hasSopId: cols.has("sop_id"),
    hasSopTitle: cols.has("sop_title"),
    hasSopUrl: cols.has("sop_url"),
    hasBillingDependency: cols.has("billing_dependency"),
    hasWorkflowState: cols.has("workflow_state"),
    hasPriority: cols.has("priority"),
    hasChecklist: cols.has("checklist"),
    hasTemplateKey: cols.has("template_key"),
    hasRecurringRule: cols.has("recurring_rule"),
    hasSlaDueAt: cols.has("sla_due_at"),
    hasDependencyIds: cols.has("dependency_ids"),
    hasBillingAction: cols.has("billing_action"),
    hasInvoiceIssueStatus: cols.has("invoice_issue_status"),
    hasAutomationSource: cols.has("automation_source"),
  };
}

async function ensureTasksTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id text NOT NULL,
      title text NOT NULL,
      description text,
      created_at timestamptz DEFAULT now()
    )
  `);

  const alterStatements = [
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status text DEFAULT 'open'`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS source text DEFAULT 'local'`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS clickup_task_id text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee_ids jsonb DEFAULT '[]'::jsonb`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS due_date timestamptz`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sop_id text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sop_title text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sop_url text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS billing_dependency text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workflow_state text DEFAULT 'active'`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority text DEFAULT 'medium'`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS checklist jsonb DEFAULT '[]'::jsonb`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS template_key text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recurring_rule jsonb`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_due_at timestamptz`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS dependency_ids jsonb DEFAULT '[]'::jsonb`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS billing_action jsonb`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS invoice_issue_status text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS automation_source text`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()`,
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()`,
  ];

  for (const sql of alterStatements) {
    try {
      await query(sql);
    } catch (e: any) {
      const msg = String(e?.message || "");
      if (
        e?.code === "42501" ||
        /must be owner of table/i.test(msg) ||
        /permission denied/i.test(msg)
      ) {
        console.warn("[tasks] schema alter skipped:", msg);
        break;
      }
      throw e;
    }
  }

  return getTasksSchemaInfo();
}

async function ensureTaskAttachmentsTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS task_attachments (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
      client_id TEXT NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
      file_name TEXT NOT NULL,
      file_url TEXT NOT NULL,
      storage_path TEXT NOT NULL,
      content_type TEXT,
      file_size BIGINT,
      uploaded_by UUID,
      source TEXT DEFAULT 'nlm_os',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `).catch((e: any) => {
    console.warn("[tasks] ensure task_attachments skipped:", e?.message || e);
  });

  await query(`
    CREATE INDEX IF NOT EXISTS idx_task_attachments_task_id
    ON task_attachments(task_id)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_task_attachments_client_id
    ON task_attachments(client_id)
  `).catch(() => null);
}

async function resolveClientName(clientId: string) {
  const id = String(clientId || "").trim();
  if (!id) return "";

  const titleFromSlug = id
    .split("-")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");

  try {
    const colsRes = await query(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'clients'
      `,
    );

    const cols = new Set(
      (colsRes.rows || []).map((r: any) => String(r.column_name || "").trim()),
    );

    const candidateNameCols = [
      "name",
      "client_name",
      "display_name",
      "business_name",
      "title",
    ].filter((c) => cols.has(c));

    const whereSlug = cols.has("slug");
    const whereId = cols.has("id");

    if (!candidateNameCols.length) {
      return titleFromSlug;
    }

    const nameExpr = `COALESCE(${candidateNameCols.join(", ")})`;

    if (whereSlug) {
      const bySlug = await query(
        `
        SELECT ${nameExpr} AS name
        FROM clients
        WHERE slug = $1
        LIMIT 1
        `,
        [id],
      );

      if (bySlug.rows?.[0]?.name) {
        return String(bySlug.rows[0].name).trim();
      }
    }

    if (whereId) {
      const byId = await query(
        `
        SELECT ${nameExpr} AS name
        FROM clients
        WHERE id::text = $1
        LIMIT 1
        `,
        [id],
      );

      if (byId.rows?.[0]?.name) {
        return String(byId.rows[0].name).trim();
      }
    }
  } catch (e: any) {
    console.warn("Client name lookup failed", e?.message || e);
  }

  return titleFromSlug;
}

function normalizeTaskIdentityPart(value: any) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

function buildTaskIdentityKey(task: any) {
  const clickupId = String(task?.clickup_task_id || task?.id || "").trim();
  if (clickupId) return `clickup:${clickupId}`;

  const clientKey = normalizeTaskIdentityPart(
    task?.client_id || task?.client_name || "",
  );
  const titleKey = normalizeTaskIdentityPart(task?.title || task?.name || "");
  const descKey = normalizeTaskIdentityPart(task?.description || "");

  if (clientKey || titleKey || descKey) {
    return `composite:${clientKey}::${titleKey}::${descKey}`;
  }

  return "";
}

function taskPreferenceScore(task: any) {
  let score = 0;

  if (String(task?.clickup_task_id || task?.id || "").trim()) score += 100;
  if (
    String(task?.source || "")
      .trim()
      .toLowerCase() === "clickup"
  )
    score += 25;
  if (String(task?.client_id || "").trim()) score += 20;
  if (String(task?.client_name || "").trim()) score += 10;
  if (String(task?.description || "").trim()) score += 5;
  if (Array.isArray(task?.assignee_ids) && task.assignee_ids.length) score += 3;
  if (String(task?.due_date || "").trim()) score += 2;
  if (String(task?.updated_at || task?.date_updated || "").trim()) score += 1;

  return score;
}

function choosePreferredTask(current: any, incoming: any) {
  if (!current) return incoming;
  if (!incoming) return current;

  const currentScore = taskPreferenceScore(current);
  const incomingScore = taskPreferenceScore(incoming);

  if (incomingScore !== currentScore) {
    return incomingScore > currentScore ? incoming : current;
  }

  const currentUpdated = new Date(
    current?.updated_at || current?.date_updated || current?.created_at || 0,
  ).getTime();
  const incomingUpdated = new Date(
    incoming?.updated_at || incoming?.date_updated || incoming?.created_at || 0,
  ).getTime();

  if (Number.isFinite(incomingUpdated) && Number.isFinite(currentUpdated)) {
    return incomingUpdated >= currentUpdated ? incoming : current;
  }

  return incoming;
}

function dedupeTasks(tasks: any[]) {
  const byKey = new Map<string, any>();
  const fallbackKeys = new Set<string>();
  const out: any[] = [];

  for (const task of Array.isArray(tasks) ? tasks : []) {
    const key = buildTaskIdentityKey(task);

    if (key) {
      byKey.set(key, choosePreferredTask(byKey.get(key), task));
      continue;
    }

    const fallbackKey = [
      normalizeTaskIdentityPart(task?.client_id || task?.client_name || ""),
      normalizeTaskIdentityPart(task?.title || task?.name || ""),
      normalizeTaskIdentityPart(task?.description || ""),
    ].join("::");

    if (fallbackKeys.has(fallbackKey)) continue;
    fallbackKeys.add(fallbackKey);
    out.push(task);
  }

  return [...Array.from(byKey.values()), ...out];
}

function extractAssigneeIds(task: any) {
  if (Array.isArray(task?.assignee_ids)) {
    return task.assignee_ids
      .map((value: any) => String(value || "").trim())
      .filter(Boolean);
  }

  if (Array.isArray(task?.assignees)) {
    return task.assignees
      .map((a: any) => String(a?.id ?? a?.userid ?? a?.user_id ?? "").trim())
      .filter(Boolean);
  }

  const single = String(
    task?.assignee_id || task?.assigneeId || task?.assignee_id_text || "",
  ).trim();

  return single ? [single] : [];
}

function normalizeDueDateInput(value: any) {
  if (value === null || value === undefined || value === "") return null;

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString();
  }

  if (typeof value === "number") {
    const ms = value < 1e12 ? value * 1000 : value;
    const date = new Date(ms);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  const raw = String(value || "").trim();
  if (!raw) return null;

  if (/^\d{10,13}$/.test(raw)) {
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    const ms = raw.length === 10 ? n * 1000 : n;
    const date = new Date(ms);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  // IMPORTANT:
  // Keep date-only values at noon UTC so they don't drift backward
  // when parsed/rendered by clients in other timezones.
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const date = new Date(`${raw}T12:00:00.000Z`);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function extractAssignee(task: any) {
  const direct =
    task?.assignee?.username ||
    task?.assignee?.email ||
    task?.assignee?.name ||
    task?.assignee ||
    "";

  if (direct && typeof direct === "string") {
    return direct.trim();
  }

  if (Array.isArray(task?.assignees) && task.assignees.length) {
    const names = task.assignees
      .map(
        (a: any) =>
          a?.username || a?.email || a?.name || a?.initials || String(a || ""),
      )
      .filter(Boolean)
      .map((v: any) => String(v).trim())
      .filter(Boolean);

    if (names.length) return names.join(", ");
  }

  return "";
}

function normalizeDbTask(row: any, schema: TasksSchemaInfo) {
  const assigneeIds =
    schema.hasAssigneeIds && Array.isArray(row?.assignee_ids)
      ? row.assignee_ids
      : schema.hasAssigneeIds && typeof row?.assignee_ids === "string"
        ? (() => {
            try {
              const parsed = JSON.parse(row.assignee_ids);
              return Array.isArray(parsed) ? parsed : [];
            } catch {
              return [];
            }
          })()
        : [];

  return {
    ...row,
    name: row?.name || row?.title || "Untitled Task",
    title: row?.title || row?.name || "Untitled Task",
    description: row?.description || "",
    status: schema.hasStatus ? row.status || "open" : "open",
    source: schema.hasSource ? row.source || "local" : "local",
    clickup_task_id: schema.hasClickupTaskId
      ? row.clickup_task_id || null
      : null,
    assignee: schema.hasAssignee ? row.assignee || "" : "",
    assignee_ids: assigneeIds,
    due_date: schema.hasDueDate ? row.due_date || null : null,
    sop_id: schema.hasSopId ? row.sop_id || null : null,
    sop_title: schema.hasSopTitle ? row.sop_title || null : null,
    sop_url: schema.hasSopUrl ? row.sop_url || null : null,
    billing_dependency: schema.hasBillingDependency
      ? row.billing_dependency || null
      : null,
    workflow_state: schema.hasWorkflowState
      ? normalizeWorkflowState(row.workflow_state || row.status || "active")
      : normalizeWorkflowState(row.status || "active"),
    priority: schema.hasPriority
      ? normalizePriority(row.priority || "medium")
      : "medium",
    checklist: schema.hasChecklist ? normalizeJsonArray(row.checklist) : [],
    template_key: schema.hasTemplateKey ? row.template_key || null : null,
    recurring_rule: schema.hasRecurringRule
      ? normalizeJsonObject(row.recurring_rule)
      : null,
    recurring_summary: schema.hasRecurringRule
      ? summarizeRecurringRule(row.recurring_rule)
      : "",
    sla_due_at: schema.hasSlaDueAt ? row.sla_due_at || null : null,
    dependency_ids: schema.hasDependencyIds
      ? parseDependencyIds(row.dependency_ids)
      : [],
    billing_action: schema.hasBillingAction
      ? normalizeJsonObject(row.billing_action)
      : null,
    invoice_issue_status: schema.hasInvoiceIssueStatus
      ? row.invoice_issue_status || null
      : null,
    automation_source: schema.hasAutomationSource
      ? row.automation_source || null
      : null,
  };
}

async function findTaskByAnyId(taskId: string) {
  const id = String(taskId || "").trim();
  if (!id) return null;

  const found = await query(
    `
    SELECT *
    FROM tasks
    WHERE id::text = $1
       OR clickup_task_id = $1
    LIMIT 1
    `,
    [id],
  );

  return found.rows?.[0] || null;
}

function isClosingStatus(value: any) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return ["complete", "completed", "closed"].includes(normalized);
}

async function clientExistsForTask(clientId: string) {
  const id = String(clientId || "").trim();
  if (!id) return false;

  if (/^whmcs-\d+$/i.test(id)) {
    const whmcsId = Number(String(id).replace(/^whmcs-/i, ""));
    const foundWhmcs = await query(
      `
      SELECT 1
      FROM whmcs_clients_cache
      WHERE whmcs_client_id = $1
      LIMIT 1
      `,
      [whmcsId],
    ).catch(() => ({ rows: [] }) as any);
    return Boolean(foundWhmcs.rows?.[0]);
  }

  const foundLocal = await query(
    `
    SELECT 1
    FROM clients
    WHERE id::text = $1
    LIMIT 1
    `,
    [id],
  ).catch(() => ({ rows: [] }) as any);

  if (foundLocal.rows?.[0]) return true;

  const foundWhmcsBySynthetic = await query(
    `
    SELECT 1
    FROM whmcs_clients_cache
    WHERE whmcs_client_id::text = $1
    LIMIT 1
    `,
    [id],
  ).catch(() => ({ rows: [] }) as any);

  return Boolean(foundWhmcsBySynthetic.rows?.[0]);
}

async function getClientBillingBlockInfo(clientId: string) {
  const id = String(clientId || "").trim();
  if (!id) return { blocked: false, reason: "" };

  let whmcsClientId: number | null = null;

  if (/^whmcs-\d+$/i.test(id)) {
    whmcsClientId = Number(String(id).replace(/^whmcs-/i, ""));
  } else {
    const mapped = await query(
      `
      SELECT whmcs_client_id
      FROM clients
      WHERE id::text = $1
      LIMIT 1
      `,
      [id],
    ).catch(() => ({ rows: [] }) as any);
    whmcsClientId = Number(mapped.rows?.[0]?.whmcs_client_id || 0) || null;
  }

  if (!whmcsClientId) return { blocked: false, reason: "" };

  const invoiceAgg = await query(
    `
    SELECT
      COUNT(*) FILTER (
        WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Unpaid', 'Overdue', 'Payment Pending'])
      )::int AS overdue_invoices,
      COALESCE(SUM(
        CASE
          WHEN COALESCE(status, '') ILIKE 'Paid' THEN 0
          ELSE COALESCE(balance, total, 0)
        END
      ), 0) AS balance_due
    FROM whmcs_invoices_cache
    WHERE whmcs_client_id = $1
    `,
    [whmcsClientId],
  ).catch(() => ({ rows: [{ overdue_invoices: 0, balance_due: 0 }] }) as any);

  const overdueInvoices = Number(invoiceAgg.rows?.[0]?.overdue_invoices || 0);
  const balanceDue = Number(invoiceAgg.rows?.[0]?.balance_due || 0);

  if (overdueInvoices > 0 || balanceDue > 0) {
    return {
      blocked: true,
      reason:
        overdueInvoices > 0
          ? "No task close if invoice unpaid"
          : "Billing dependency unresolved",
    };
  }

  return { blocked: false, reason: "" };
}

async function clientHasSops(clientId: string) {
  const id = String(clientId || "").trim();
  if (!id) return false;

  const result = await query(
    `
    SELECT COUNT(*)::int AS total
    FROM sops
    WHERE client_id = $1
    `,
    [id],
  ).catch((e: any) => {
    if (e?.code === "42P01") return { rows: [{ total: 0 }] } as any;
    throw e;
  });

  return Number(result.rows?.[0]?.total || 0) > 0;
}

async function resolveSopReference(args: {
  clientId: string;
  sopId?: string | null;
  sopTitle?: string | null;
  sopUrl?: string | null;
}) {
  const clientId = String(args.clientId || "").trim();
  const incomingId = String(args.sopId || "").trim();
  const incomingTitle = String(args.sopTitle || "").trim();
  const incomingUrl = String(args.sopUrl || "").trim();

  const fallback = {
    sopId: incomingId || null,
    sopTitle: incomingTitle || null,
    sopUrl: incomingUrl || null,
    source: incomingId || incomingTitle || incomingUrl ? "manual" : null,
    found: false,
  };

  if (!clientId) return fallback;

  try {
    if (incomingId) {
      const byId = await query(
        `
        SELECT id, title, url, source
        FROM sops
        WHERE client_id = $1
          AND id::text = $2
        LIMIT 1
        `,
        [clientId, incomingId],
      );
      const row = byId.rows?.[0];
      if (row) {
        return {
          sopId: String(row.id || incomingId),
          sopTitle: String(row.title || incomingTitle || "").trim() || null,
          sopUrl: String(row.url || incomingUrl || "").trim() || null,
          source: String(row.source || "manual").trim() || "manual",
          found: true,
        };
      }
    }

    if (incomingTitle || incomingUrl) {
      const byLoose = await query(
        `
        SELECT id, title, url, source
        FROM sops
        WHERE client_id = $1
          AND (
            ($2 <> '' AND LOWER(COALESCE(title, '')) = LOWER($2))
            OR ($3 <> '' AND COALESCE(url, '') = $3)
          )
        ORDER BY created_at DESC NULLS LAST
        LIMIT 1
        `,
        [clientId, incomingTitle, incomingUrl],
      );
      const row = byLoose.rows?.[0];
      if (row) {
        return {
          sopId: String(row.id || incomingId || "").trim() || null,
          sopTitle: String(row.title || incomingTitle || "").trim() || null,
          sopUrl: String(row.url || incomingUrl || "").trim() || null,
          source: String(row.source || "manual").trim() || "manual",
          found: true,
        };
      }
    }
  } catch (e: any) {
    if (e?.code !== "42P01") {
      console.warn("[tasks] SOP lookup skipped:", e?.message || e);
    }
  }

  return fallback;
}

function getTaskSopState(taskLike: any, hasClientSops: boolean) {
  const sopId = String(taskLike?.sop_id || "").trim();
  const sopTitle = String(taskLike?.sop_title || "").trim();
  const sopUrl = String(taskLike?.sop_url || "").trim();
  const linked = Boolean(sopId || sopTitle || sopUrl);
  const gap = hasClientSops && !linked;

  return {
    sop_required: hasClientSops,
    sop_linked: linked,
    sop_gap: gap,
    sop_recommendation: gap
      ? "Attach an SOP reference to keep execution consistent."
      : linked
        ? "SOP linked."
        : null,
    sharepoint_fallback: !isSharePointConfigured(),
  };
}

function normalizeTaskTitleForPattern(value: any) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ");
}

function taskNeedsSopPolicy(taskLike: any) {
  const title = normalizeTaskTitleForPattern(
    taskLike?.title || taskLike?.name || "",
  );
  const billingDependency = String(taskLike?.billing_dependency || "")
    .trim()
    .toLowerCase();
  const keywords = [
    "deploy",
    "deployment",
    "server",
    "backup",
    "restore",
    "migration",
    "integrat",
    "dns",
    "hosting",
    "email setup",
    "domain",
    "ssl",
    "firewall",
    "infrastructure",
  ];

  return (
    keywords.some((keyword) => title.includes(keyword)) ||
    ["required", "sop_required"].includes(billingDependency)
  );
}

async function countRepeatedTaskPattern(clientId: string, title: string) {
  const normalized = normalizeTaskTitleForPattern(title);
  if (!clientId || !normalized) return 0;

  const r = await query(
    `
    SELECT COUNT(*)::int AS total
    FROM tasks
    WHERE client_id = $1
      AND LOWER(REGEXP_REPLACE(COALESCE(title, ''), '[^a-zA-Z0-9\s]', ' ', 'g')) = $2
    `,
    [clientId, normalized],
  ).catch(() => ({ rows: [{ total: 0 }] }) as any);

  return Number(r.rows?.[0]?.total || 0);
}

async function getClientRecentSops(clientId: string, limit = 5) {
  const r = await query(
    `
    SELECT id, title, url, COALESCE(source, 'manual') AS source, created_at
    FROM sops
    WHERE client_id = $1
    ORDER BY created_at DESC NULLS LAST
    LIMIT $2
    `,
    [clientId, limit],
  ).catch((e: any) => {
    if (e?.code === "42P01") return { rows: [] } as any;
    throw e;
  });

  return Array.isArray(r.rows) ? r.rows : [];
}

async function getClientSopAnalytics(clientId: string) {
  const result = await query(
    `
    SELECT
      COUNT(*)::int AS total_tasks,
      COUNT(*) FILTER (
        WHERE COALESCE(NULLIF(TRIM(COALESCE(sop_id, '')), ''), NULLIF(TRIM(COALESCE(sop_title, '')), ''), NULLIF(TRIM(COALESCE(sop_url, '')), '')) IS NOT NULL
      )::int AS linked_tasks,
      COUNT(*) FILTER (
        WHERE COALESCE(NULLIF(TRIM(COALESCE(sop_id, '')), ''), NULLIF(TRIM(COALESCE(sop_title, '')), ''), NULLIF(TRIM(COALESCE(sop_url, '')), '')) IS NULL
      )::int AS no_sop_tasks
    FROM tasks
    WHERE client_id = $1
    `,
    [clientId],
  ).catch(
    () =>
      ({ rows: [{ total_tasks: 0, linked_tasks: 0, no_sop_tasks: 0 }] }) as any,
  );

  const mostUsed = await query(
    `
    SELECT
      COALESCE(NULLIF(TRIM(sop_title), ''), NULLIF(TRIM(sop_url), ''), 'Linked SOP') AS label,
      COUNT(*)::int AS count
    FROM tasks
    WHERE client_id = $1
      AND COALESCE(NULLIF(TRIM(COALESCE(sop_id, '')), ''), NULLIF(TRIM(COALESCE(sop_title, '')), ''), NULLIF(TRIM(COALESCE(sop_url, '')), '')) IS NOT NULL
    GROUP BY 1
    ORDER BY count DESC, label ASC
    LIMIT 5
    `,
    [clientId],
  ).catch(() => ({ rows: [] }) as any);

  return {
    total_tasks: Number(result.rows?.[0]?.total_tasks || 0),
    linked_tasks: Number(result.rows?.[0]?.linked_tasks || 0),
    no_sop_tasks: Number(result.rows?.[0]?.no_sop_tasks || 0),
    most_used_sops: Array.isArray(mostUsed.rows) ? mostUsed.rows : [],
  };
}

async function buildEnhancedTaskSopState(taskLike: any, clientId: string) {
  const hasClientSops = await clientHasSops(clientId);
  const base = getTaskSopState(taskLike, hasClientSops);
  const policyRequired = taskNeedsSopPolicy(taskLike);
  const repeatedCount = await countRepeatedTaskPattern(
    clientId,
    String(taskLike?.title || taskLike?.name || ""),
  );
  const analytics = await getClientSopAnalytics(clientId);
  const recentSops = await getClientRecentSops(clientId, 5);

  return {
    ...base,
    sop_required_by_policy: policyRequired,
    sop_acknowledged: Boolean(taskLike?.sop_acknowledged ?? base.sop_linked),
    sop_block_completion: policyRequired && !base.sop_linked,
    repeated_task_pattern_count: repeatedCount,
    repeated_task_missing_sop_warning:
      repeatedCount >= 2 && !base.sop_linked
        ? "This task pattern repeats often and still has no SOP linked."
        : null,
    recent_client_sops: recentSops,
    sop_usage_analytics: analytics,
  };
}

async function syncDbWithClickUp(args: {
  clientId: string;
  schema: TasksSchemaInfo;
  remoteTasks: any[];
}) {
  const { clientId, schema, remoteTasks } = args;
  if (!schema.hasClickupTaskId || !schema.hasSource || !schema.hasStatus) {
    return;
  }

  const remoteIds = remoteTasks
    .map((task) => String(task?.clickup_task_id || task?.id || "").trim())
    .filter(Boolean);

  await query(
    `
    DELETE FROM tasks
    WHERE client_id = $1
      AND source = 'clickup'
      AND clickup_task_id IS NOT NULL
      AND clickup_task_id <> ALL($2::text[])
    `,
    [clientId, remoteIds.length ? remoteIds : ["__none__"]],
  ).catch((e: any) => {
    console.warn("[tasks] ClickUp delete sync skipped:", e?.message || e);
  });

  for (const task of remoteTasks) {
    const clickupTaskId = String(
      task?.clickup_task_id || task?.id || "",
    ).trim();
    if (!clickupTaskId) continue;

    const title =
      String(task?.title || task?.name || "").trim() || "Untitled Task";
    const description = String(task?.description || "").trim();
    const status = String(task?.status || "open").trim() || "open";
    const assignee = extractAssignee(task);
    const assigneeIds = extractAssigneeIds(task);
    const dueDate = normalizeDueDateInput(task?.due_date);

    if (schema.hasAssignee) {
      await query(
        `
        INSERT INTO tasks (client_id, title, description, status, source, clickup_task_id, assignee, assignee_ids, due_date)
        SELECT $1, $2, $3, $4, 'clickup', $5, $6, $7::jsonb, $8::timestamptz
        WHERE NOT EXISTS (
          SELECT 1
          FROM tasks
          WHERE client_id = $1
            AND clickup_task_id = $5
        )
        `,
        [
          clientId,
          title,
          description,
          status,
          clickupTaskId,
          assignee,
          JSON.stringify(assigneeIds),
          dueDate,
        ],
      ).catch(() => null);

      await query(
        `
        UPDATE tasks
        SET
          title = $1,
          description = $2,
          status = $3,
          source = 'clickup',
          assignee = $4,
          assignee_ids = $5::jsonb,
          due_date = $6::timestamptz
        WHERE client_id = $7
          AND clickup_task_id = $8
        `,
        [
          title,
          description,
          status,
          assignee,
          JSON.stringify(assigneeIds),
          dueDate,
          clientId,
          clickupTaskId,
        ],
      ).catch((e: any) => {
        console.warn("[tasks] ClickUp update sync skipped:", e?.message || e);
      });

      await query(
        `
        DELETE FROM tasks a
        USING tasks b
        WHERE a.id < b.id
          AND a.client_id = b.client_id
          AND COALESCE(a.clickup_task_id, '') <> ''
          AND a.clickup_task_id = b.clickup_task_id
        `,
      ).catch(() => null);
    } else {
      await query(
        `
        INSERT INTO tasks (client_id, title, description, status, source, clickup_task_id)
        SELECT $1, $2, $3, $4, 'clickup', $5
        WHERE NOT EXISTS (
          SELECT 1
          FROM tasks
          WHERE client_id = $1
            AND clickup_task_id = $5
        )
        `,
        [clientId, title, description, status, clickupTaskId],
      ).catch(() => null);

      await query(
        `
        UPDATE tasks
        SET
          title = $1,
          description = $2,
          status = $3,
          source = 'clickup'
        WHERE client_id = $4
          AND clickup_task_id = $5
        `,
        [title, description, status, clientId, clickupTaskId],
      ).catch((e: any) => {
        console.warn("[tasks] ClickUp update sync skipped:", e?.message || e);
      });
    }
  }
}

function buildMirrorSummary(results: AttachmentMirrorResult[]) {
  const attempted = results.length;
  const mirrored = results.filter((r) => r.mirrored).length;
  const failed = attempted - mirrored;

  return {
    attempted,
    mirrored,
    failed,
    ok: failed === 0,
    results,
  };
}

router.get("/templates", requireAuth, async (_req: any, res) => {
  return res.json({ ok: true, templates: TASK_TEMPLATE_LIBRARY });
});

router.get("/workload", requireAuth, async (_req: any, res) => {
  const schema = await ensureTasksTable();
  const rows = await query(
    `SELECT * FROM tasks ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST`,
  ).catch(() => ({ rows: [] }) as any);
  const normalized = (rows.rows || []).map((row: any) =>
    normalizeDbTask(row, schema),
  );
  const byOwner = new Map<string, any>();
  for (const task of normalized) {
    const owners =
      Array.isArray(task.assignee_ids) && task.assignee_ids.length
        ? task.assignee_ids
        : ["unassigned"];
    for (const ownerId of owners) {
      const current = byOwner.get(ownerId) || {
        owner_id: ownerId,
        task_count: 0,
        blocked_count: 0,
        overdue_count: 0,
        waiting_count: 0,
        clients: new Set<string>(),
      };
      current.task_count += 1;
      if (String(task.workflow_state || "").trim() === "blocked")
        current.blocked_count += 1;
      if (
        task.due_date &&
        new Date(task.due_date).getTime() < Date.now() &&
        !isClosingStatus(task.status)
      )
        current.overdue_count += 1;
      if (String(task.workflow_state || "").trim() === "waiting_on_client")
        current.waiting_count += 1;
      if (task.client_id) current.clients.add(String(task.client_id));
      byOwner.set(ownerId, current);
    }
  }
  const items = Array.from(byOwner.values()).map((item: any) => ({
    owner_id: item.owner_id,
    task_count: item.task_count,
    blocked_count: item.blocked_count,
    overdue_count: item.overdue_count,
    waiting_count: item.waiting_count,
    client_count: item.clients.size,
  }));
  return res.json({ ok: true, items });
});

router.post("/intake", requireAuth, async (req: any, res) => {
  const clientId = String(req.body?.clientId || "").trim();
  const templateKey = String(req.body?.templateKey || "").trim();
  const titlePrefix = String(req.body?.titlePrefix || "").trim();
  const template = getTemplateRecord(templateKey);
  if (!clientId || !template) {
    return res.status(400).json({
      ok: false,
      error: "clientId and valid templateKey are required",
    });
  }
  const schema = await ensureTasksTable();
  const created: any[] = [];
  for (const item of template.checklist) {
    const columns = ["client_id", "title", "description"];
    const values: any[] = [
      clientId,
      `${titlePrefix || template.label}: ${item.label}`,
      template.description || "",
    ];
    if (schema.hasStatus) {
      columns.push("status");
      values.push("open");
    }
    if (schema.hasSource) {
      columns.push("source");
      values.push("local");
    }
    if (schema.hasWorkflowState) {
      columns.push("workflow_state");
      values.push(normalizeWorkflowState(template.workflow_state || "intake"));
    }
    if (schema.hasChecklist) {
      columns.push("checklist");
      values.push(
        JSON.stringify(
          template.checklist.map((entry) => ({ ...entry, done: false })),
        ),
      );
    }
    if (schema.hasTemplateKey) {
      columns.push("template_key");
      values.push(template.key);
    }
    if (schema.hasRecurringRule) {
      columns.push("recurring_rule");
      values.push(
        template.recurring_rule
          ? JSON.stringify(template.recurring_rule)
          : null,
      );
    }
    if (schema.hasBillingAction) {
      columns.push("billing_action");
      values.push(
        template.billing_action
          ? JSON.stringify(template.billing_action)
          : null,
      );
    }
    if (schema.hasAutomationSource) {
      columns.push("automation_source");
      values.push("intake_form");
    }
    const placeholders = columns.map((col, index) => {
      const cast = ["checklist", "recurring_rule", "billing_action"].includes(
        col,
      )
        ? "::jsonb"
        : "";
      return `$${index + 1}${cast}`;
    });
    const result = await query(
      `INSERT INTO tasks (${columns.join(", ")}) VALUES (${placeholders.join(", ")}) RETURNING *`,
      values,
    );
    created.push(normalizeDbTask(result.rows[0], schema));
  }
  return res.json({ ok: true, tasks: created, template });
});

router.get("/assignees", requireAuth, async (req: any, res) => {
  try {
    if (!hasClickUp()) {
      return res.json({ ok: true, assignees: [] });
    }

    const assignees = await listClickUpAssignees({
      clientId: String(req.query?.clientId || "").trim(),
    });

    return res.json({ ok: true, assignees });
  } catch (e: any) {
    console.warn("ClickUp assignees fetch failed", e?.message || e);
    return res.status(502).json({
      ok: false,
      error: e?.message || "Failed to fetch ClickUp assignees",
      assignees: [],
    });
  }
});

router.get("/", async (req, res) => {
  const clientId = String(req.query.clientId || "").trim();
  const loadAll =
    clientId.toLowerCase() === "all" || clientId.toLowerCase() === "__all__";

  if (!clientId && !loadAll) {
    return res.status(400).json({ ok: false, error: "clientId required" });
  }

  const schema = await ensureTasksTable();
  const orderBy = schema.hasCreatedAt
    ? "client_id ASC, created_at DESC"
    : "client_id ASC, title ASC";

  let remote: any[] = [];

  try {
    if (hasClickUp()) {
      if (loadAll) {
        remote = await clickupListTasks({});
      } else {
        remote = await clickupListTasks({
          clientId,
          tag: `client:${clientId}`,
        });
      }

      if (loadAll) {
        const grouped = new Map<string, any[]>();

        for (const task of remote) {
          const key = String(task?.client_id || "").trim();
          if (!key) continue;
          if (!grouped.has(key)) grouped.set(key, []);
          grouped.get(key)!.push(task);
        }

        for (const [groupClientId, groupTasks] of grouped.entries()) {
          await syncDbWithClickUp({
            clientId: groupClientId,
            schema,
            remoteTasks: groupTasks,
          });
        }
      } else {
        await syncDbWithClickUp({
          clientId,
          schema,
          remoteTasks: remote,
        });
      }
    }
  } catch (e: any) {
    // intentionally silent to avoid noisy polling logs
  }

  const dbTasks = loadAll
    ? await query(`SELECT * FROM tasks ORDER BY ${orderBy}`)
    : await query(
        `SELECT * FROM tasks WHERE client_id=$1 ORDER BY ${orderBy}`,
        [clientId],
      );

  const normalizedDb = (dbTasks.rows || []).map((row: any) =>
    normalizeDbTask(row, schema),
  );

  const dbByClickupId = new Map<string, any>();
  const dbByLocalId = new Map<string, any>();
  const dbByComposite = new Map<string, any>();

  for (const row of normalizedDb) {
    const clickupId = String(row?.clickup_task_id || "").trim();
    const localId = String(row?.id || "").trim();
    const composite = [
      String(row?.client_id || "")
        .trim()
        .toLowerCase(),
      String(row?.title || row?.name || "")
        .trim()
        .toLowerCase(),
      String(row?.description || "")
        .trim()
        .toLowerCase(),
    ].join("::");

    if (clickupId) dbByClickupId.set(clickupId, row);
    if (localId) dbByLocalId.set(localId, row);
    if (composite && !dbByComposite.has(composite))
      dbByComposite.set(composite, row);
  }

  const mergedRemote = remote.map((task: any) => {
    const clickupId = String(task?.clickup_task_id || task?.id || "").trim();
    const localId = String(task?.id || "").trim();
    const composite = [
      String(task?.client_id || "")
        .trim()
        .toLowerCase(),
      String(task?.title || task?.name || "")
        .trim()
        .toLowerCase(),
      String(task?.description || "")
        .trim()
        .toLowerCase(),
    ].join("::");

    const dbMatch =
      (clickupId ? dbByClickupId.get(clickupId) : null) ||
      (localId ? dbByLocalId.get(localId) : null) ||
      dbByComposite.get(composite) ||
      null;

    return {
      ...(dbMatch || {}),
      ...task,
      id:
        String(task?.id || dbMatch?.id || "").trim() || dbMatch?.id || task?.id,
      clickup_task_id:
        String(
          task?.clickup_task_id || task?.id || dbMatch?.clickup_task_id || "",
        ).trim() ||
        dbMatch?.clickup_task_id ||
        task?.clickup_task_id ||
        task?.id ||
        null,
      client_id:
        String(task?.client_id || dbMatch?.client_id || "").trim() ||
        String(dbMatch?.client_id || "").trim(),
      client_name:
        String(task?.client_name || dbMatch?.client_name || "").trim() ||
        String(dbMatch?.client_name || "").trim(),
      due_date: task?.due_date ?? dbMatch?.due_date ?? null,
    };
  });

  const tasks = dedupeTasks(
    [...mergedRemote, ...normalizedDb].map((task: any) => ({
      ...task,
      name: task?.name || task?.title || "Untitled Task",
      title: task?.title || task?.name || "Untitled Task",
      description: task?.description || "",
      assignee: task?.assignee || extractAssignee(task) || "",
      assignee_ids: extractAssigneeIds(task),
      due_date: normalizeDueDateInput(task?.due_date),
      client_id: String(task?.client_id || "").trim(),
      client_name: String(task?.client_name || "").trim(),
    })),
  );

  for (const task of tasks) {
    if (
      !String(task?.client_name || "").trim() &&
      String(task?.client_id || "").trim()
    ) {
      task.client_name = await resolveClientName(String(task.client_id));
    }
  }

  const tasksWithSopState = await Promise.all(
    tasks.map(async (task: any) => ({
      ...task,
      ...(await buildEnhancedTaskSopState(
        task,
        String(task?.client_id || clientId || "").trim(),
      )),
    })),
  );

  res.json({
    ok: true,
    tasks: tasksWithSopState,
    provider: hasClickUp() ? "clickup" : "local",
    scope: loadAll ? "all" : clientId,
  });
});

router.post("/", requireAuth, async (req: any, res) => {
  const clientId = String(req.body?.clientId || "").trim();
  const title = String(req.body?.title || "").trim();
  const description = String(req.body?.description || "").trim();
  const assignee = String(req.body?.assignee || "").trim();
  const assigneeId = String(req.body?.assigneeId || "").trim();
  const assigneeEmail = normalizeEmail(req.body?.assigneeEmail || "");
  const assigneeIds = Array.isArray(req.body?.assigneeIds)
    ? req.body.assigneeIds
        .map((value: any) => String(value || "").trim())
        .filter(Boolean)
    : assigneeId
      ? [assigneeId]
      : [];
  const dueDate = normalizeDueDateInput(
    req.body?.dueDate ?? req.body?.due_date,
  );
  const sopIdInput = String(req.body?.sopId || "").trim();
  const sopTitleInput = String(req.body?.sopTitle || "").trim();
  const sopUrlInput = String(req.body?.sopUrl || "").trim();
  const billingDependency = String(req.body?.billingDependency || "").trim();
  const workflowState = normalizeWorkflowState(
    req.body?.workflowState ||
      req.body?.workflow_state ||
      req.body?.status ||
      "active",
  );
  const priority = normalizePriority(req.body?.priority || "medium");
  const templateKey = String(req.body?.templateKey || "").trim();
  const checklist = buildChecklistFromTemplate(
    templateKey,
    req.body?.checklist,
  );
  const recurringRule = normalizeJsonObject(
    req.body?.recurringRule ?? req.body?.recurring_rule,
  );
  const slaDueAt = normalizeDueDateInput(
    req.body?.slaDueAt ?? req.body?.sla_due_at ?? dueDate,
  );
  const dependencyIds = parseDependencyIds(
    req.body?.dependencyIds ?? req.body?.dependency_ids,
  );
  const billingAction = normalizeJsonObject(
    req.body?.billingAction ?? req.body?.billing_action,
  );
  const invoiceIssueStatus =
    String(req.body?.invoiceIssueStatus || "").trim() || null;
  const automationSource =
    String(req.body?.automationSource || "").trim() || null;

  if (!clientId || !title) {
    return res.status(400).json({ ok: false, error: "Missing fields" });
  }

  const schema = await ensureTasksTable();

  if (!(await clientExistsForTask(clientId))) {
    return res.status(400).json({
      ok: false,
      error: "Tasks must be linked to a valid client. No orphan tasks allowed.",
    });
  }

  const resolvedSop = await resolveSopReference({
    clientId,
    sopId: sopIdInput,
    sopTitle: sopTitleInput,
    sopUrl: sopUrlInput,
  });
  const sopId = String(resolvedSop.sopId || "").trim();
  const sopTitle = String(resolvedSop.sopTitle || "").trim();
  const sopUrl = String(resolvedSop.sopUrl || "").trim();

  if (
    ((await clientHasSops(clientId)) ||
      taskNeedsSopPolicy({ title, billing_dependency: billingDependency })) &&
    !sopId &&
    !sopTitle &&
    !sopUrl
  ) {
    return res.status(400).json({
      ok: false,
      error: "This task requires an SOP reference before task creation.",
    });
  }

  const clientName = await resolveClientName(clientId);

  let clickup: any = null;

  try {
    if (hasClickUp()) {
      clickup = await clickupCreateTask({
        title,
        description,
        tag: `client:${clientId}`,
        clientId,
        clientName,
        assigneeIds: assigneeIds.length ? assigneeIds : undefined,
        dueDate,
      });
    }
  } catch (e: any) {
    console.warn("ClickUp sync failed", e?.message || e);
  }

  let existing: any = null;

  if (clickup?.id && schema.hasClickupTaskId) {
    const found = await query(
      `
      SELECT *
      FROM tasks
      WHERE client_id = $1
        AND clickup_task_id = $2
      LIMIT 1
      `,
      [clientId, clickup.id],
    );
    existing = found.rows?.[0] || null;
  }

  if (existing) {
    const updateSets = [
      `title = $1`,
      `description = $2`,
      `status = $3`,
      `source = $4`,
    ];
    const updateValues: any[] = [
      title,
      description || "",
      String(clickup?.status || "not started").trim() || "not started",
      clickup?.id ? "clickup" : "local",
    ];

    if (schema.hasAssignee) {
      updateValues.push(extractAssignee(clickup) || assignee);
      updateSets.push(`assignee = $${updateValues.length}`);
    }

    if (schema.hasAssigneeIds) {
      updateValues.push(
        JSON.stringify(extractAssigneeIds(clickup) || assigneeIds),
      );
      updateSets.push(`assignee_ids = $${updateValues.length}::jsonb`);
    }

    if (schema.hasDueDate) {
      updateValues.push(clickup?.due_date || dueDate || null);
      updateSets.push(`due_date = $${updateValues.length}`);
    }
    if (schema.hasWorkflowState) {
      updateValues.push(workflowState);
      updateSets.push(`workflow_state = $${updateValues.length}`);
    }
    if (schema.hasPriority) {
      updateValues.push(priority);
      updateSets.push(`priority = $${updateValues.length}`);
    }
    if (schema.hasChecklist) {
      updateValues.push(JSON.stringify(checklist));
      updateSets.push(`checklist = $${updateValues.length}::jsonb`);
    }
    if (schema.hasTemplateKey) {
      updateValues.push(templateKey || null);
      updateSets.push(`template_key = $${updateValues.length}`);
    }
    if (schema.hasRecurringRule) {
      updateValues.push(recurringRule ? JSON.stringify(recurringRule) : null);
      updateSets.push(`recurring_rule = $${updateValues.length}::jsonb`);
    }
    if (schema.hasSlaDueAt) {
      updateValues.push(slaDueAt || null);
      updateSets.push(`sla_due_at = $${updateValues.length}`);
    }
    if (schema.hasDependencyIds) {
      updateValues.push(JSON.stringify(dependencyIds));
      updateSets.push(`dependency_ids = $${updateValues.length}::jsonb`);
    }
    if (schema.hasBillingAction) {
      updateValues.push(billingAction ? JSON.stringify(billingAction) : null);
      updateSets.push(`billing_action = $${updateValues.length}::jsonb`);
    }
    if (schema.hasInvoiceIssueStatus) {
      updateValues.push(invoiceIssueStatus);
      updateSets.push(`invoice_issue_status = $${updateValues.length}`);
    }
    if (schema.hasAutomationSource) {
      updateValues.push(automationSource || "manual");
      updateSets.push(`automation_source = $${updateValues.length}`);
    }

    updateValues.push(existing.id);

    const updateSql = `
      UPDATE tasks
      SET ${updateSets.join(", ")}, updated_at = NOW()
      WHERE id = $${updateValues.length}
      RETURNING *
    `;

    const updated = await query(updateSql, updateValues);
    const task = normalizeDbTask(updated.rows[0], schema);

    const responseTask = {
      ...task,
      ...(clickup?.id
        ? {
            status:
              String(clickup?.status || "not started").trim() || "not started",
            source: "clickup",
            clickup_task_id: clickup.id,
            assignee: extractAssignee(clickup) || assignee,
            assignee_ids: extractAssigneeIds(clickup) || assigneeIds,
            due_date: clickup?.due_date || dueDate || null,
          }
        : {}),
    };

    const finalTask = {
      ...responseTask,
      ...getTaskSopState(responseTask, await clientHasSops(clientId)),
    };

    if (assigneeIds.length || assignee) {
      await notifyTaskAssignees({
        task: finalTask,
        clientName,
        actorName: String(req.user?.name || req.user?.email || "NLM OS").trim(),
        assigneeIds: extractAssigneeIds(finalTask),
        assigneeLabel: String(finalTask?.assignee || assignee || "").trim(),
        assigneeEmail,
        reason: "created",
      }).catch(() => null);
    }

    return res.json({
      ok: true,
      task: finalTask,
    });
  }

  const insertColumns = ["client_id", "title", "description"];
  const insertValues: any[] = [clientId, title, description || ""];

  if (schema.hasStatus) {
    insertColumns.push("status");
    insertValues.push(
      String(
        clickup?.status || (clickup?.id ? "not started" : "open"),
      ).trim() || (clickup?.id ? "not started" : "open"),
    );
  }

  if (schema.hasSource) {
    insertColumns.push("source");
    insertValues.push(clickup?.id ? "clickup" : "local");
  }

  if (schema.hasClickupTaskId) {
    insertColumns.push("clickup_task_id");
    insertValues.push(clickup?.id || null);
  }

  if (schema.hasAssignee) {
    insertColumns.push("assignee");
    insertValues.push(extractAssignee(clickup) || assignee);
  }

  if (schema.hasAssigneeIds) {
    insertColumns.push("assignee_ids");
    insertValues.push(
      JSON.stringify(extractAssigneeIds(clickup) || assigneeIds),
    );
  }

  if (schema.hasDueDate) {
    insertColumns.push("due_date");
    insertValues.push(clickup?.due_date || dueDate || null);
  }

  if (schema.hasSopId) {
    insertColumns.push("sop_id");
    insertValues.push(sopId || null);
  }

  if (schema.hasSopTitle) {
    insertColumns.push("sop_title");
    insertValues.push(sopTitle || null);
  }

  if (schema.hasSopUrl) {
    insertColumns.push("sop_url");
    insertValues.push(sopUrl || null);
  }

  if (schema.hasBillingDependency) {
    insertColumns.push("billing_dependency");
    insertValues.push(billingDependency || null);
  }
  if (schema.hasWorkflowState) {
    insertColumns.push("workflow_state");
    insertValues.push(workflowState);
  }
  if (schema.hasPriority) {
    insertColumns.push("priority");
    insertValues.push(priority);
  }
  if (schema.hasChecklist) {
    insertColumns.push("checklist");
    insertValues.push(JSON.stringify(checklist));
  }
  if (schema.hasTemplateKey) {
    insertColumns.push("template_key");
    insertValues.push(templateKey || null);
  }
  if (schema.hasRecurringRule) {
    insertColumns.push("recurring_rule");
    insertValues.push(recurringRule ? JSON.stringify(recurringRule) : null);
  }
  if (schema.hasSlaDueAt) {
    insertColumns.push("sla_due_at");
    insertValues.push(slaDueAt || null);
  }
  if (schema.hasDependencyIds) {
    insertColumns.push("dependency_ids");
    insertValues.push(JSON.stringify(dependencyIds));
  }
  if (schema.hasBillingAction) {
    insertColumns.push("billing_action");
    insertValues.push(billingAction ? JSON.stringify(billingAction) : null);
  }
  if (schema.hasInvoiceIssueStatus) {
    insertColumns.push("invoice_issue_status");
    insertValues.push(invoiceIssueStatus);
  }
  if (schema.hasAutomationSource) {
    insertColumns.push("automation_source");
    insertValues.push(automationSource || "manual");
  }

  const insertSql = `
    INSERT INTO tasks (${insertColumns.join(", ")})
    VALUES (${insertValues.map((_, index) => `$${index + 1}${["checklist", "recurring_rule", "dependency_ids", "billing_action"].includes(insertColumns[index]) ? "::jsonb" : ""}`).join(", ")})
    RETURNING *
  `;

  const r = await query(insertSql, insertValues);
  const task = normalizeDbTask(r.rows[0], schema);

  const responseTask = {
    ...task,
    ...(clickup?.id
      ? {
          status:
            String(clickup?.status || "not started").trim() || "not started",
          source: "clickup",
          clickup_task_id: clickup.id,
          assignee: extractAssignee(clickup) || assignee,
          assignee_ids: extractAssigneeIds(clickup) || assigneeIds,
          due_date: clickup?.due_date || dueDate || null,
          workflow_state: workflowState,
          priority,
          checklist,
          template_key: templateKey || null,
          recurring_rule: recurringRule,
          recurring_summary: summarizeRecurringRule(recurringRule),
          sla_due_at: slaDueAt || null,
          dependency_ids: dependencyIds,
          billing_action: billingAction,
          invoice_issue_status: invoiceIssueStatus,
          automation_source: automationSource || "manual",
        }
      : {}),
  };

  const finalTask = {
    ...responseTask,
    ...(await buildEnhancedTaskSopState(responseTask, clientId)),
  };

  if (assigneeIds.length || assignee || assigneeEmail) {
    await notifyTaskAssignees({
      task: finalTask,
      clientName,
      actorName: String(req.user?.name || req.user?.email || "NLM OS").trim(),
      assigneeIds: extractAssigneeIds(finalTask),
      assigneeLabel: String(finalTask?.assignee || assignee || "").trim(),
      assigneeEmail,
      reason: "created",
    }).catch(() => null);
  }

  res.json({
    ok: true,
    task: finalTask,
  });
});

router.get("/:id/attachments", requireAuth, async (req: any, res) => {
  const id = String(req.params?.id || "").trim();

  if (!id) {
    return res.status(400).json({ ok: false, error: "id is required" });
  }

  await ensureTasksTable();
  await ensureTaskAttachmentsTable();

  const task = await findTaskByAnyId(id);
  if (!task) {
    return res.status(404).json({ ok: false, error: "Task not found" });
  }

  const rows = await query(
    `
    SELECT *
    FROM task_attachments
    WHERE task_id = $1
    ORDER BY created_at DESC, id DESC
    `,
    [task.id],
  );

  return res.json({
    ok: true,
    attachments: rows.rows || [],
  });
});

router.post(
  "/:id/attachments",
  requireAuth,
  upload.array("files", 10),
  async (req: any, res) => {
    const id = String(req.params?.id || "").trim();

    if (!id) {
      return res.status(400).json({ ok: false, error: "id is required" });
    }

    await ensureTasksTable();
    await ensureTaskAttachmentsTable();

    const task = await findTaskByAnyId(id);
    if (!task) {
      return res.status(404).json({ ok: false, error: "Task not found" });
    }

    const files = Array.isArray(req.files) ? req.files : [];
    if (!files.length) {
      return res.status(400).json({ ok: false, error: "No files uploaded" });
    }

    const created: any[] = [];
    const mirrorResults: AttachmentMirrorResult[] = [];

    for (const file of files as Express.Multer.File[]) {
      const storagePath = buildTaskAttachmentStoragePath({
        clientId: String(task.client_id || ""),
        taskId: String(task.id || ""),
        fileName: String(file.originalname || "file"),
      });

      const uploaded = await uploadBufferToBunny({
        storagePath,
        buffer: file.buffer,
        contentType: file.mimetype || "application/octet-stream",
      });

      const inserted = await query(
        `
        INSERT INTO task_attachments
          (task_id, client_id, file_name, file_url, storage_path, content_type, file_size)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        `,
        [
          task.id,
          task.client_id,
          file.originalname,
          uploaded.fileUrl,
          uploaded.storagePath,
          file.mimetype || null,
          Number(file.size || 0),
        ],
      );

      const attachment = inserted.rows?.[0];
      if (attachment) created.push(attachment);

      if (task.clickup_task_id && hasClickUp()) {
        try {
          await clickupUploadAttachment({
            taskId: String(task.clickup_task_id),
            fileName: file.originalname,
            contentType: file.mimetype || "application/octet-stream",
            buffer: file.buffer,
          });

          mirrorResults.push({
            fileName: file.originalname,
            mirrored: true,
            taskId: String(task.clickup_task_id),
            error: null,
            status: 200,
          });
        } catch (e: any) {
          const message =
            e?.message ||
            e?.data?.err ||
            e?.data?.error ||
            e?.data?.message ||
            "Failed to mirror attachment to ClickUp";

          mirrorResults.push({
            fileName: file.originalname,
            mirrored: false,
            taskId: String(task.clickup_task_id),
            error: message,
            status: Number(e?.status || 0) || null,
          });
        }
      } else {
        mirrorResults.push({
          fileName: file.originalname,
          mirrored: false,
          taskId: task.clickup_task_id ? String(task.clickup_task_id) : null,
          error: task.clickup_task_id
            ? "ClickUp is not configured"
            : "Task is not linked to ClickUp",
          status: null,
        });
      }
    }

    await query(`UPDATE tasks SET updated_at = NOW() WHERE id = $1`, [
      task.id,
    ]).catch(() => null);

    const clickupMirror = buildMirrorSummary(
      mirrorResults.filter(
        (r) => String(r.taskId || "").trim() !== "" || !!r.error,
      ),
    );

    const partialMirrorFailure =
      !!task.clickup_task_id &&
      clickupMirror.attempted > 0 &&
      clickupMirror.failed > 0;

    return res.json({
      ok: true,
      attachments: created,
      clickupMirror,
      warning: partialMirrorFailure
        ? "Attachment uploaded to NLM OS, but one or more files failed to mirror to ClickUp."
        : null,
      message:
        !!task.clickup_task_id && clickupMirror.attempted > 0
          ? partialMirrorFailure
            ? "Attachment uploaded with ClickUp mirror warning."
            : "Attachment uploaded and mirrored to ClickUp."
          : "Attachment uploaded successfully.",
    });
  },
);

router.delete(
  "/:id/attachments/:attachmentId",
  requireAuth,
  async (req: any, res) => {
    const id = String(req.params?.id || "").trim();
    const attachmentId = String(req.params?.attachmentId || "").trim();

    if (!id || !attachmentId) {
      return res.status(400).json({
        ok: false,
        error: "id and attachmentId are required",
      });
    }

    const task = await findTaskByAnyId(id);
    if (!task) {
      return res.status(404).json({ ok: false, error: "Task not found" });
    }

    const found = await query(
      `
      SELECT *
      FROM task_attachments
      WHERE id = $1
        AND task_id = $2
      LIMIT 1
      `,
      [attachmentId, task.id],
    );

    const attachment = found.rows?.[0];
    if (!attachment) {
      return res.status(404).json({ ok: false, error: "Attachment not found" });
    }

    if (attachment.storage_path) {
      await deleteFromBunny(String(attachment.storage_path));
    }

    await query(`DELETE FROM task_attachments WHERE id = $1`, [attachment.id]);
    await query(`UPDATE tasks SET updated_at = NOW() WHERE id = $1`, [
      task.id,
    ]).catch(() => null);

    return res.json({ ok: true, id: attachment.id });
  },
);

router.patch("/:id", requireAuth, async (req: any, res) => {
  const id = String(req.params?.id || "").trim();
  const title = String(req.body?.title || "").trim();
  const description = String(req.body?.description || "").trim();
  const assignee = String(req.body?.assignee || "").trim();
  const assigneeId = String(req.body?.assigneeId || "").trim();
  const assigneeEmail = normalizeEmail(req.body?.assigneeEmail || "");
  const assigneeIdsInput = Array.isArray(req.body?.assigneeIds)
    ? req.body.assigneeIds
    : assigneeId
      ? [assigneeId]
      : [];
  const assigneeIds = assigneeIdsInput
    .map((value: any) => String(value || "").trim())
    .filter(Boolean);
  const statusRaw = String(req.body?.status || "").trim();
  const dueDate = normalizeDueDateInput(
    req.body?.dueDate ?? req.body?.due_date,
  );
  const clientId =
    req.body?.clientId != null ? String(req.body.clientId || "").trim() : "";
  const sopIdInput =
    req.body?.sopId != null ? String(req.body.sopId || "").trim() : "";
  const sopTitleInput =
    req.body?.sopTitle != null ? String(req.body.sopTitle || "").trim() : "";
  const sopUrlInput =
    req.body?.sopUrl != null ? String(req.body.sopUrl || "").trim() : "";
  const billingDependency =
    req.body?.billingDependency != null
      ? String(req.body.billingDependency || "").trim()
      : "";
  const workflowStateRaw =
    req.body?.workflowState != null || req.body?.workflow_state != null
      ? String(req.body?.workflowState || req.body?.workflow_state || "").trim()
      : "";
  const priorityRaw =
    req.body?.priority != null ? String(req.body.priority || "").trim() : "";
  const templateKey =
    req.body?.templateKey != null
      ? String(req.body.templateKey || "").trim()
      : "";
  const checklistInput =
    req.body?.checklist != null
      ? buildChecklistFromTemplate(
          templateKey || String((req as any)?.body?.templateKey || ""),
          req.body.checklist,
        )
      : null;
  const recurringRuleInput =
    req.body?.recurringRule != null || req.body?.recurring_rule != null
      ? normalizeJsonObject(req.body?.recurringRule ?? req.body?.recurring_rule)
      : null;
  const slaDueAtInput =
    req.body?.slaDueAt != null || req.body?.sla_due_at != null
      ? normalizeDueDateInput(req.body?.slaDueAt ?? req.body?.sla_due_at)
      : null;
  const dependencyIdsInput =
    req.body?.dependencyIds != null || req.body?.dependency_ids != null
      ? parseDependencyIds(req.body?.dependencyIds ?? req.body?.dependency_ids)
      : null;
  const billingActionInput =
    req.body?.billingAction != null || req.body?.billing_action != null
      ? normalizeJsonObject(req.body?.billingAction ?? req.body?.billing_action)
      : null;
  const invoiceIssueStatusInput =
    req.body?.invoiceIssueStatus != null
      ? String(req.body.invoiceIssueStatus || "").trim()
      : "";
  const automationSourceInput =
    req.body?.automationSource != null
      ? String(req.body.automationSource || "").trim()
      : "";
  const sopAcknowledged =
    req.body?.sopAcknowledged != null
      ? Boolean(req.body.sopAcknowledged)
      : false;

  if (!id) {
    return res.status(400).json({ ok: false, error: "id is required" });
  }

  const schema = await ensureTasksTable();
  const task = await findTaskByAnyId(id);

  if (!task) {
    return res.status(404).json({ ok: false, error: "Task not found" });
  }

  const nextTitle = title || task.title || task.name || "Untitled Task";
  const nextDescription =
    req.body?.description !== undefined ? description : task.description || "";

  let nextAssignee =
    req.body?.assignee !== undefined
      ? assignee
      : schema.hasAssignee
        ? task.assignee || ""
        : "";

  let nextStatus = statusRaw || task.status || "open";

  let nextDueDate =
    req.body?.dueDate !== undefined || req.body?.due_date !== undefined
      ? dueDate
      : normalizeDueDateInput(task?.due_date);

  let nextAssigneeIds =
    Array.isArray(req.body?.assigneeIds) || req.body?.assigneeId !== undefined
      ? assigneeIds
      : extractAssigneeIds(task);
  const nextWorkflowState = workflowStateRaw
    ? normalizeWorkflowState(workflowStateRaw)
    : normalizeWorkflowState(task?.workflow_state || task?.status || "active");
  const nextPriority = priorityRaw
    ? normalizePriority(priorityRaw)
    : normalizePriority(task?.priority || "medium");
  const nextTemplateKey =
    req.body?.templateKey !== undefined
      ? templateKey
      : String(task?.template_key || "").trim();
  const nextChecklist =
    checklistInput !== null
      ? checklistInput
      : buildChecklistFromTemplate(nextTemplateKey, task?.checklist);
  const nextRecurringRule =
    recurringRuleInput !== null
      ? recurringRuleInput
      : normalizeJsonObject(task?.recurring_rule);
  const nextSlaDueAt =
    slaDueAtInput !== null
      ? slaDueAtInput
      : normalizeDueDateInput(task?.sla_due_at || task?.due_date);
  const nextDependencyIds =
    dependencyIdsInput !== null
      ? dependencyIdsInput
      : parseDependencyIds(task?.dependency_ids);
  const nextBillingAction =
    billingActionInput !== null
      ? billingActionInput
      : normalizeJsonObject(task?.billing_action);
  const nextInvoiceIssueStatus =
    req.body?.invoiceIssueStatus !== undefined
      ? invoiceIssueStatusInput || null
      : task?.invoice_issue_status || null;
  const nextAutomationSource =
    req.body?.automationSource !== undefined
      ? automationSourceInput || null
      : task?.automation_source || null;

  const nextClientId =
    req.body?.clientId !== undefined
      ? clientId
      : String(task?.client_id || "").trim();

  if (!nextClientId || !(await clientExistsForTask(nextClientId))) {
    return res.status(400).json({
      ok: false,
      error:
        "Tasks must stay linked to a valid client. No orphan tasks allowed.",
    });
  }

  const resolvedSop = await resolveSopReference({
    clientId: nextClientId,
    sopId:
      req.body?.sopId !== undefined ? sopIdInput : String(task?.sop_id || ""),
    sopTitle:
      req.body?.sopTitle !== undefined
        ? sopTitleInput
        : String(task?.sop_title || ""),
    sopUrl:
      req.body?.sopUrl !== undefined
        ? sopUrlInput
        : String(task?.sop_url || ""),
  });
  const sopId = String(resolvedSop.sopId || "").trim();
  const sopTitle = String(resolvedSop.sopTitle || "").trim();
  const sopUrl = String(resolvedSop.sopUrl || "").trim();

  if (
    ((await clientHasSops(nextClientId)) ||
      taskNeedsSopPolicy({
        title: nextTitle,
        billing_dependency:
          billingDependency || String(task?.billing_dependency || "").trim(),
      })) &&
    !(
      sopId ||
      task?.sop_id ||
      sopTitle ||
      task?.sop_title ||
      sopUrl ||
      task?.sop_url
    )
  ) {
    return res.status(400).json({
      ok: false,
      error: "This task requires an SOP reference before saving the task.",
    });
  }

  let nextClientName = nextClientId
    ? await resolveClientName(nextClientId)
    : String(task?.client_name || "").trim();

  const billingBlockInfo = await getClientBillingBlockInfo(nextClientId);
  const requiresSopBeforeClose = taskNeedsSopPolicy({
    title: nextTitle,
    billing_dependency:
      billingDependency || String(task?.billing_dependency || "").trim(),
  });
  const hasResolvedSop = Boolean(sopId || sopTitle || sopUrl);
  if (
    isClosingStatus(nextStatus) &&
    requiresSopBeforeClose &&
    !hasResolvedSop &&
    !sopAcknowledged
  ) {
    return res.status(400).json({
      ok: false,
      error:
        "SOP required before completing this task. Link an SOP or acknowledge the SOP requirement first.",
      code: "SOP_REQUIRED",
    });
  }
  if (isClosingStatus(nextStatus) && billingBlockInfo.blocked) {
    return res.status(400).json({
      ok: false,
      error:
        billingBlockInfo.reason ||
        "Task completion blocked by billing dependency.",
    });
  }

  let updatedClickUp: any = null;

  if (task.clickup_task_id && hasClickUp()) {
    try {
      updatedClickUp = await updateClickUpTask({
        taskId: task.clickup_task_id,
        title: nextTitle,
        description: nextDescription,
        status: nextStatus,
        assigneeIds: nextAssigneeIds,
        dueDate: nextDueDate,
        clientId: nextClientId || undefined,
        clientName: nextClientName || undefined,
      });

      nextStatus =
        String(updatedClickUp?.status || nextStatus).trim() || nextStatus;

      nextAssignee = extractAssignee(updatedClickUp) || nextAssignee || "";

      const refreshedAssigneeIds = extractAssigneeIds(updatedClickUp);
      if (Array.isArray(refreshedAssigneeIds) && refreshedAssigneeIds.length) {
        nextAssigneeIds = refreshedAssigneeIds;
      } else if (
        Array.isArray(req.body?.assigneeIds) ||
        req.body?.assigneeId !== undefined
      ) {
        nextAssigneeIds = assigneeIds;
      }

      const refreshedDueDate = normalizeDueDateInput(updatedClickUp?.due_date);
      if (req.body?.dueDate !== undefined || req.body?.due_date !== undefined) {
        nextDueDate = refreshedDueDate ?? dueDate;
      } else if (refreshedDueDate) {
        nextDueDate = refreshedDueDate;
      }

      const refreshedClientId = String(
        updatedClickUp?.client_id || nextClientId || "",
      ).trim();
      const refreshedClientName = String(
        updatedClickUp?.client_name || nextClientName || "",
      ).trim();

      if (refreshedClientId) {
        nextClientName =
          refreshedClientName || (await resolveClientName(refreshedClientId));
      }
    } catch (e: any) {
      console.warn("ClickUp task update failed", e?.message || e);
      return res.status(502).json({
        ok: false,
        error: e?.message || "Failed to update ClickUp task",
      });
    }
  }

  const updateSets = [
    `client_id = $1`,
    `title = $2`,
    `description = $3`,
    `status = $4`,
    `updated_at = NOW()`,
  ];
  const values: any[] = [nextClientId, nextTitle, nextDescription, nextStatus];

  if (schema.hasAssignee) {
    updateSets.push(`assignee = $${values.length + 1}`);
    values.push(nextAssignee);
  }

  if (schema.hasAssigneeIds) {
    updateSets.push(`assignee_ids = $${values.length + 1}::jsonb`);
    values.push(JSON.stringify(nextAssigneeIds));
  }

  if (schema.hasDueDate) {
    updateSets.push(`due_date = $${values.length + 1}::timestamptz`);
    values.push(nextDueDate);
  }

  if (schema.hasSopId) {
    updateSets.push(`sop_id = $${values.length + 1}`);
    values.push(sopId || task?.sop_id || null);
  }

  if (schema.hasSopTitle) {
    updateSets.push(`sop_title = $${values.length + 1}`);
    values.push(sopTitle || task?.sop_title || null);
  }

  if (schema.hasSopUrl) {
    updateSets.push(`sop_url = $${values.length + 1}`);
    values.push(sopUrl || task?.sop_url || null);
  }

  if (schema.hasBillingDependency) {
    updateSets.push(`billing_dependency = $${values.length + 1}`);
    values.push(
      billingDependency ||
        (billingBlockInfo.blocked
          ? billingBlockInfo.reason || "Billing dependency unresolved"
          : task?.billing_dependency || null),
    );
  }
  if (schema.hasWorkflowState) {
    updateSets.push(`workflow_state = $${values.length + 1}`);
    values.push(nextWorkflowState);
  }
  if (schema.hasPriority) {
    updateSets.push(`priority = $${values.length + 1}`);
    values.push(nextPriority);
  }
  if (schema.hasChecklist) {
    updateSets.push(`checklist = $${values.length + 1}::jsonb`);
    values.push(JSON.stringify(nextChecklist));
  }
  if (schema.hasTemplateKey) {
    updateSets.push(`template_key = $${values.length + 1}`);
    values.push(nextTemplateKey || null);
  }
  if (schema.hasRecurringRule) {
    updateSets.push(`recurring_rule = $${values.length + 1}::jsonb`);
    values.push(nextRecurringRule ? JSON.stringify(nextRecurringRule) : null);
  }
  if (schema.hasSlaDueAt) {
    updateSets.push(`sla_due_at = $${values.length + 1}`);
    values.push(nextSlaDueAt || null);
  }
  if (schema.hasDependencyIds) {
    updateSets.push(`dependency_ids = $${values.length + 1}::jsonb`);
    values.push(JSON.stringify(nextDependencyIds));
  }
  if (schema.hasBillingAction) {
    updateSets.push(`billing_action = $${values.length + 1}::jsonb`);
    values.push(nextBillingAction ? JSON.stringify(nextBillingAction) : null);
  }
  if (schema.hasInvoiceIssueStatus) {
    updateSets.push(`invoice_issue_status = $${values.length + 1}`);
    values.push(nextInvoiceIssueStatus);
  }
  if (schema.hasAutomationSource) {
    updateSets.push(`automation_source = $${values.length + 1}`);
    values.push(nextAutomationSource);
  }

  values.push(task.id);

  const setClause = updateSets.join(", ");
  const updated = await query(
    `
    UPDATE tasks
    SET ${setClause}
    WHERE id = $${values.length}
    RETURNING *
    `,
    values,
  );

  const normalized = normalizeDbTask(updated.rows[0], schema);

  const responseTask = {
    ...normalized,
    clickup_task_id:
      task.clickup_task_id || normalized?.clickup_task_id || null,
    client_name: nextClientName || String(normalized?.client_name || "").trim(),
    assignee: nextAssignee,
    assignee_ids: nextAssigneeIds,
    due_date: nextDueDate,
    sop_id: sopId || task?.sop_id || null,
    sop_title: sopTitle || task?.sop_title || null,
    sop_url: sopUrl || task?.sop_url || null,
    billing_dependency:
      billingDependency ||
      (billingBlockInfo.blocked
        ? billingBlockInfo.reason || "Billing dependency unresolved"
        : task?.billing_dependency || null),
    workflow_state: nextWorkflowState,
    priority: nextPriority,
    checklist: nextChecklist,
    template_key: nextTemplateKey || null,
    recurring_rule: nextRecurringRule,
    recurring_summary: summarizeRecurringRule(nextRecurringRule),
    sla_due_at: nextSlaDueAt || null,
    dependency_ids: nextDependencyIds,
    billing_action: nextBillingAction,
    invoice_issue_status: nextInvoiceIssueStatus,
    automation_source: nextAutomationSource,
    ...(updatedClickUp
      ? {
          status: nextStatus,
          assignees: Array.isArray(updatedClickUp?.assignees)
            ? updatedClickUp.assignees
            : [],
        }
      : {}),
  };

  const finalTask = {
    ...responseTask,
    ...getTaskSopState(responseTask, await clientHasSops(nextClientId)),
  };

  const shouldNotifyAssignee =
    Array.isArray(req.body?.assigneeIds) ||
    req.body?.assigneeId !== undefined ||
    req.body?.assignee !== undefined;

  if (shouldNotifyAssignee) {
    await notifyTaskAssignees({
      task: finalTask,
      clientName: nextClientName,
      actorName: String(req.user?.name || req.user?.email || "NLM OS").trim(),
      assigneeIds: extractAssigneeIds(finalTask),
      assigneeLabel: String(finalTask?.assignee || nextAssignee || "").trim(),
      reason: "assigned",
    }).catch(() => null);
  }

  res.json({
    ok: true,
    task: finalTask,
  });
});

router.post("/:taskId/share", requireAuth, async (req: any, res) => {
  try {
    const taskId = String(req.params?.taskId || "").trim();
    const email = normalizeEmail(req.body?.email);
    const message = String(req.body?.message || "").trim();

    if (!taskId || !email) {
      return res
        .status(400)
        .json({ ok: false, error: "Task ID and recipient email are required" });
    }

    await ensureTasksTable();
    const task = await findTaskByAnyId(taskId);
    if (!task) {
      return res.status(404).json({ ok: false, error: "Task not found" });
    }

    const clientName = await resolveClientName(String(task.client_id || ""));
    const shareUrl =
      String(req.body?.shareUrl || "").trim() || buildTaskAppLink(task);
    const clickupUrl =
      String(task?.url || "").trim() ||
      buildClickUpTaskUrl(String(task?.clickup_task_id || ""));

    const emailResult = await sendTaskShareEmail({
      to: email,
      taskTitle: String(task.title || task.name || "Untitled Task"),
      clientName,
      sharedByName: String(req.user?.name || req.user?.email || "NLM OS"),
      shareUrl: shareUrl || clickupUrl || "",
      message,
    });

    const recipientAdmins = await findAdminsByEmails([email]);
    const recipientAdmin = recipientAdmins[0] || null;
    if (recipientAdmin?.id) {
      await createNotification({
        userId: String(recipientAdmin.id),
        kind: "task_share",
        title: "A task was shared with you",
        body: String(task.title || task.name || "Untitled Task"),
        actionUrl: shareUrl || null,
        actionLabel: clickupUrl ? "Open task" : "Open task",
        meta: {
          task_id: String(task.id || ""),
          clickup_task_id: String(task.clickup_task_id || ""),
          client_id: String(task.client_id || ""),
          clickup_url: clickupUrl || null,
          shared_by: String(req.user?.id || ""),
        },
      }).catch(() => null);
    }

    return res.json({
      ok: true,
      email_sent: Boolean(emailResult?.ok),
      share_url: shareUrl,
      clickup_url: clickupUrl || null,
      task: {
        id: String(task.id || ""),
        clickup_task_id: String(task.clickup_task_id || "") || null,
        client_id: String(task.client_id || "") || null,
        title: String(task.title || task.name || "Untitled Task"),
      },
    });
  } catch (e: any) {
    console.error("[tasks share]", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to share task" });
  }
});

router.patch("/:id/status", requireAuth, async (req: any, res) => {
  const id = String(req.params?.id || "").trim();
  const status = String(req.body?.status || "").trim();

  if (!id || !status) {
    return res
      .status(400)
      .json({ ok: false, error: "id and status are required" });
  }

  const schema = await ensureTasksTable();
  const task = await findTaskByAnyId(id);

  if (!task) {
    return res.status(404).json({ ok: false, error: "Task not found" });
  }

  const clientTaskId = String(task?.client_id || "").trim();
  const billingBlockInfo = await getClientBillingBlockInfo(clientTaskId);
  const enhancedSopState = await buildEnhancedTaskSopState(task, clientTaskId);
  if (
    isClosingStatus(status) &&
    enhancedSopState.sop_required_by_policy &&
    !enhancedSopState.sop_linked &&
    !Boolean(req.body?.sopAcknowledged)
  ) {
    return res.status(400).json({
      ok: false,
      error:
        "SOP required before completing this task. Link an SOP or acknowledge the SOP requirement first.",
      code: "SOP_REQUIRED",
    });
  }
  if (isClosingStatus(status) && billingBlockInfo.blocked) {
    return res.status(400).json({
      ok: false,
      error:
        billingBlockInfo.reason ||
        "Task completion blocked by billing dependency.",
    });
  }

  let clickupTask: any = null;

  try {
    if (task.clickup_task_id && hasClickUp()) {
      clickupTask = await updateClickUpTaskStatus({
        taskId: task.clickup_task_id,
        status,
      });
    }
  } catch (e: any) {
    console.warn("ClickUp status update failed", e?.message || e);
    return res.status(502).json({
      ok: false,
      error: e?.message || "Failed to update ClickUp status",
    });
  }

  const nextStatus = String(clickupTask?.status || status).trim() || status;

  const updated = await query(
    `
    UPDATE tasks
    SET status = $1, updated_at = NOW()
    WHERE id = $2
    RETURNING *
    `,
    [nextStatus, task.id],
  );

  const responseTask = normalizeDbTask(updated.rows[0], schema);

  res.json({
    ok: true,
    task: {
      ...responseTask,
      ...(await buildEnhancedTaskSopState(
        responseTask,
        String(task?.client_id || "").trim(),
      )),
    },
  });
});

router.delete("/:id", requireAuth, async (req: any, res) => {
  const id = String(req.params?.id || "").trim();

  if (!id) {
    return res.status(400).json({ ok: false, error: "id is required" });
  }

  const task = await findTaskByAnyId(id);

  if (!task) {
    return res.status(404).json({ ok: false, error: "Task not found" });
  }

  try {
    if (task.clickup_task_id && hasClickUp()) {
      await deleteClickUpTask(task.clickup_task_id);
    }
  } catch (e: any) {
    console.warn("ClickUp delete failed", e?.message || e);
    return res.status(502).json({
      ok: false,
      error: e?.message || "Failed to delete ClickUp task",
    });
  }

  await query(`DELETE FROM tasks WHERE id = $1`, [task.id]);

  res.json({ ok: true, id: task.id });
});

router.get("/sop/analytics/:clientId", requireAuth, async (req: any, res) => {
  const clientId = String(req.params?.clientId || "").trim();
  if (!clientId) {
    return res.status(400).json({ ok: false, error: "clientId is required" });
  }

  const analytics = await getClientSopAnalytics(clientId);
  const recent = await getClientRecentSops(clientId, 5);
  return res.json({
    ok: true,
    clientId,
    ...analytics,
    recent_sops: recent,
  });
});

export default router;
