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
} from "../integrations/clickup.js";

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
};

type AttachmentMirrorResult = {
  fileName: string;
  mirrored: boolean;
  taskId?: string | null;
  error?: string | null;
  status?: number | null;
};

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

function dedupeTasks(tasks: any[]) {
  const seen = new Set<string>();
  const out: any[] = [];

  for (const task of tasks) {
    const key =
      String(task?.clickup_task_id || task?.id || "").trim() ||
      [
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

    if (seen.has(key)) continue;
    seen.add(key);
    out.push(task);
  }

  return out;
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

  const tasks = dedupeTasks([...mergedRemote, ...normalizedDb]).map(
    (task: any) => ({
      ...task,
      name: task?.name || task?.title || "Untitled Task",
      title: task?.title || task?.name || "Untitled Task",
      description: task?.description || "",
      assignee: task?.assignee || extractAssignee(task) || "",
      assignee_ids: extractAssigneeIds(task),
      due_date: normalizeDueDateInput(task?.due_date),
      client_id: String(task?.client_id || "").trim(),
      client_name: String(task?.client_name || "").trim(),
    }),
  );

  for (const task of tasks) {
    if (
      !String(task?.client_name || "").trim() &&
      String(task?.client_id || "").trim()
    ) {
      task.client_name = await resolveClientName(String(task.client_id));
    }
  }

  res.json({
    ok: true,
    tasks,
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

  if (!clientId || !title) {
    return res.status(400).json({ ok: false, error: "Missing fields" });
  }

  const schema = await ensureTasksTable();
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

    updateValues.push(existing.id);

    const updateSql = `
      UPDATE tasks
      SET ${updateSets.join(", ")}, updated_at = NOW()
      WHERE id = $${updateValues.length}
      RETURNING *
    `;

    const updated = await query(updateSql, updateValues);
    const task = normalizeDbTask(updated.rows[0], schema);

    return res.json({
      ok: true,
      task: {
        ...task,
        ...(clickup?.id
          ? {
              status:
                String(clickup?.status || "not started").trim() ||
                "not started",
              source: "clickup",
              clickup_task_id: clickup.id,
              assignee: extractAssignee(clickup) || assignee,
              assignee_ids: extractAssigneeIds(clickup) || assigneeIds,
              due_date: clickup?.due_date || dueDate || null,
            }
          : {}),
      },
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

  const insertSql = `
    INSERT INTO tasks (${insertColumns.join(", ")})
    VALUES (${insertValues.map((_, index) => `$${index + 1}`).join(", ")})
    RETURNING *
  `;

  const r = await query(insertSql, insertValues);
  const task = normalizeDbTask(r.rows[0], schema);

  res.json({
    ok: true,
    task: {
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
    },
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

  const nextClientId =
    req.body?.clientId !== undefined
      ? clientId
      : String(task?.client_id || "").trim();

  let nextClientName = nextClientId
    ? await resolveClientName(nextClientId)
    : String(task?.client_name || "").trim();

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

  res.json({
    ok: true,
    task: {
      ...normalized,
      clickup_task_id:
        task.clickup_task_id || normalized?.clickup_task_id || null,
      client_name:
        nextClientName || String(normalized?.client_name || "").trim(),
      assignee: nextAssignee,
      assignee_ids: nextAssigneeIds,
      due_date: nextDueDate,
      ...(updatedClickUp
        ? {
            status: nextStatus,
            assignees: Array.isArray(updatedClickUp?.assignees)
              ? updatedClickUp.assignees
              : [],
          }
        : {}),
    },
  });
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

  res.json({
    ok: true,
    task: normalizeDbTask(updated.rows[0], schema),
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

export default router;
