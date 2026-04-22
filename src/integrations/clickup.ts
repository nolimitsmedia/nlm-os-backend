// services/api/src/integrations/clickup.ts

export class ClickUpConfigError extends Error {
  code: string;
  status?: number;
  data?: any;

  constructor(message: string, code = "CLICKUP_NOT_CONFIGURED") {
    super(message);
    this.name = "ClickUpConfigError";
    this.code = code;
  }
}

const BASE = "https://api.clickup.com/api/v2";

function getToken() {
  return String(process.env.CLICKUP_TOKEN || "").trim();
}

function getDefaultListId() {
  const a = String(process.env.CLICKUP_DEFAULT_LIST_ID || "").trim();
  const b = String(process.env.CLICKUP_LIST_ID || "").trim();
  return a || b;
}

function getClientFieldId() {
  return String(process.env.CLICKUP_CLIENT_FIELD_ID || "").trim();
}

export function hasClickUp() {
  return Boolean(getToken() && getDefaultListId());
}

function authHeaders() {
  return {
    Authorization: getToken(),
    "Content-Type": "application/json",
  };
}

function normalizeString(value: any) {
  return String(value ?? "").trim();
}

function normalizeLower(value: any) {
  return normalizeString(value).toLowerCase();
}

function toClickUpDueDate(value: any): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  const raw = String(value).trim();
  if (!raw) return undefined;

  if (/^\d{10,13}$/.test(raw)) {
    const n = Number(raw);
    if (!Number.isFinite(n)) return undefined;
    return raw.length === 10 ? n * 1000 : n;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const d = new Date(`${raw}T00:00:00.000Z`);
    const t = d.getTime();
    return Number.isNaN(t) ? undefined : t;
  }

  const d = new Date(raw);
  const t = d.getTime();
  return Number.isNaN(t) ? undefined : t;
}

function extractErrorMessage(data: any, status?: number) {
  if (typeof data === "string" && data.trim()) return data.trim();
  return (
    data?.err ||
    data?.error ||
    data?.message ||
    data?.ECODE ||
    `ClickUp error${status ? ` ${status}` : ""}`
  );
}

function createClickUpError(message: string, status?: number, data?: any) {
  const err: any = new Error(message);
  err.status = status;
  err.data = data;
  return err;
}

async function parseResponseBody(res: Response) {
  const raw = await res.text().catch(() => "");
  let data: any = null;

  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {
    data = raw;
  }

  return { raw, data };
}

async function cuFetch(path: string, init: RequestInit = {}) {
  const token = getToken();
  const listId = getDefaultListId();

  if (!token || !listId) {
    throw new ClickUpConfigError(
      "ClickUp is not configured (missing token or default list id).",
      "CLICKUP_NOT_CONFIGURED",
    );
  }

  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      ...(init.headers || {}),
      ...authHeaders(),
    },
  });

  const { data } = await parseResponseBody(res);

  if (!res.ok) {
    throw createClickUpError(
      extractErrorMessage(data, res.status),
      res.status,
      data,
    );
  }

  return data;
}

type ListArgs =
  | string
  | {
      listId?: string;
      tag?: string;
      clientId?: string;
    };

type CreateArgs = {
  title: string;
  description?: string;
  listId?: string;
  tag?: string;
  clientId?: string;
  clientName?: string;
  assigneeIds?: (string | number)[];
};

type UpdateArgs = {
  taskId: string;
  title?: string;
  description?: string;
  status?: string;
  assigneeIds?: (string | number)[];
  dueDate?: string | number | null;
  clientName?: string;
  clientId?: string;
};

type ClickUpField = {
  id?: string;
  name?: string;
  type?: string;
  value?: any;
};

function extractTags(task: any): string[] {
  if (!Array.isArray(task?.tags)) return [];
  return task.tags
    .map((t: any) => String(t?.name || t?.tag || "").trim())
    .filter(Boolean);
}

function getTaskCustomFields(task: any): ClickUpField[] {
  return Array.isArray(task?.custom_fields) ? task.custom_fields : [];
}

function getClientFieldsByName(fields: ClickUpField[]): ClickUpField[] {
  return fields.filter((f: any) => normalizeLower(f?.name) === "client");
}

function getFieldValueAsString(field: ClickUpField | undefined): string {
  if (!field || field.value == null) return "";
  if (typeof field.value === "string") return field.value.trim();
  if (Array.isArray(field.value)) {
    return field.value
      .map((x) => normalizeString(x))
      .filter(Boolean)
      .join(", ");
  }
  if (typeof field.value === "object") {
    return normalizeString(
      (field.value as any)?.name ??
        (field.value as any)?.label ??
        (field.value as any)?.value ??
        "",
    );
  }
  return normalizeString(field.value);
}

function extractClientValue(task: any): string {
  const clientFieldId = getClientFieldId();
  const fields = getTaskCustomFields(task);

  if (!fields.length) return "";

  if (clientFieldId) {
    const byId = fields.find(
      (f: any) => normalizeString(f?.id) === clientFieldId,
    );
    const idValue = getFieldValueAsString(byId);
    if (idValue) return idValue;
  }

  for (const field of getClientFieldsByName(fields)) {
    const value = getFieldValueAsString(field);
    if (value) return value;
  }

  return "";
}

function extractAssignee(task: any): string {
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

function extractAssigneeIds(task: any): (string | number)[] {
  if (!Array.isArray(task?.assignees)) return [];
  return task.assignees
    .map((a: any) => a?.id ?? a?.userid ?? a?.user_id ?? null)
    .filter(
      (v: any) => v !== null && v !== undefined && String(v).trim() !== "",
    );
}

function normalizeTask(t: any) {
  const tags = extractTags(t);
  const clientFieldValue = extractClientValue(t);
  const clientTagValue =
    tags.find((x) => x.startsWith("client:"))?.replace(/^client:/, "") || "";
  const dueDateMs = toClickUpDueDate(t?.due_date);

  return {
    id: normalizeString(t?.id),
    name: normalizeString(t?.name),
    title: normalizeString(t?.name),
    description: normalizeString(t?.description),
    status: normalizeString(t?.status?.status || t?.status || ""),
    url: normalizeString(t?.url),
    due_date: dueDateMs ? new Date(Number(dueDateMs)).toISOString() : null,
    date_created: t?.date_created || null,
    date_updated: t?.date_updated || null,
    updated_at: t?.date_updated || null,
    source: "clickup",
    tags,
    assignee: extractAssignee(t),
    assignees: Array.isArray(t?.assignees) ? t.assignees : [],
    assignee_ids: extractAssigneeIds(t),
    client_id: clientTagValue || clientFieldValue || "",
    client_name: clientFieldValue || clientTagValue || "",
    clickup_task_id: normalizeString(t?.id),
  };
}

function buildNormalizedTaskIdentity(task: any) {
  const clickupId = normalizeString(task?.clickup_task_id || task?.id);
  if (clickupId) return `clickup:${clickupId}`;

  const clientKey = normalizeLower(task?.client_id || task?.client_name);
  const titleKey = normalizeLower(task?.title || task?.name);
  const descKey = normalizeLower(task?.description);

  if (clientKey || titleKey || descKey) {
    return `composite:${clientKey}::${titleKey}::${descKey}`;
  }

  return "";
}

function taskSortScore(task: any) {
  let score = 0;
  if (normalizeString(task?.clickup_task_id || task?.id)) score += 100;
  if (normalizeString(task?.client_id)) score += 20;
  if (normalizeString(task?.client_name)) score += 10;
  if (normalizeString(task?.description)) score += 5;
  if (Array.isArray(task?.assignee_ids) && task.assignee_ids.length) score += 3;
  if (normalizeString(task?.due_date)) score += 2;
  if (normalizeString(task?.updated_at || task?.date_updated)) score += 1;
  return score;
}

function dedupeNormalizedTasks(tasks: any[]) {
  const byKey = new Map<string, any>();

  for (const task of Array.isArray(tasks) ? tasks : []) {
    const normalized = normalizeTask(task);
    const key = buildNormalizedTaskIdentity(normalized);

    if (!key) {
      const fallback = `${normalizeLower(normalized?.title)}::${normalizeLower(normalized?.client_id || normalized?.client_name)}`;
      if (!byKey.has(fallback)) byKey.set(fallback, normalized);
      continue;
    }

    const existing = byKey.get(key);
    if (!existing || taskSortScore(normalized) >= taskSortScore(existing)) {
      byKey.set(key, normalized);
    }
  }

  return Array.from(byKey.values());
}

async function getListCustomFields(listId?: string) {
  const lid = normalizeString(listId || getDefaultListId());
  if (!lid) return [];

  try {
    const data = await cuFetch(`/list/${encodeURIComponent(lid)}/field`, {
      method: "GET",
    });
    return Array.isArray(data?.fields) ? data.fields : [];
  } catch (e: any) {
    console.warn("ClickUp field lookup failed", e?.message || e);
    return [];
  }
}

async function resolveClientFieldIds(listId?: string) {
  const configured = getClientFieldId();
  const fields = await getListCustomFields(listId);
  const ids = new Set<string>();

  if (configured) ids.add(configured);

  for (const field of fields) {
    const name = normalizeLower(field?.name);
    if (name === "client" && normalizeString(field?.id)) {
      ids.add(normalizeString(field.id));
    }
  }

  return Array.from(ids);
}

async function setTaskClientFields(args: {
  taskId: string;
  listId?: string;
  value: string;
}) {
  const taskId = normalizeString(args.taskId);
  const value = normalizeString(args.value);
  const fieldIds = await resolveClientFieldIds(args.listId);

  if (!taskId || !value || !fieldIds.length) return;

  for (const fieldId of fieldIds) {
    try {
      await cuFetch(
        `/task/${encodeURIComponent(taskId)}/field/${encodeURIComponent(fieldId)}`,
        {
          method: "POST",
          body: JSON.stringify({ value }),
        },
      );
    } catch (e: any) {
      console.warn(
        `ClickUp custom field set failed for ${fieldId}`,
        e?.message || e,
      );
    }
  }
}

export async function getClickUpListMembers(listId?: string) {
  const lid = normalizeString(listId || getDefaultListId());
  if (!lid) return [];

  const data = await cuFetch(`/list/${encodeURIComponent(lid)}/member`, {
    method: "GET",
  });

  const members = Array.isArray(data?.members)
    ? data.members
    : Array.isArray(data?.users)
      ? data.users
      : [];

  return members
    .map((member: any) => {
      const user = member?.user || member;
      return {
        id: String(user?.id ?? member?.id ?? "").trim(),
        username: normalizeString(user?.username),
        email: normalizeString(user?.email),
        initials: normalizeString(user?.initials),
        color: normalizeString(user?.color),
        profilePicture: normalizeString(
          user?.profilePicture || user?.profile_picture,
        ),
        displayName:
          normalizeString(user?.username) ||
          normalizeString(user?.email) ||
          normalizeString(user?.initials) ||
          normalizeString(user?.name) ||
          "Unassigned",
      };
    })
    .filter((member: any) => member.id);
}

export async function listClickUpAssignees(args?: {
  listId?: string;
  clientId?: string;
}) {
  const members = await getClickUpListMembers(args?.listId);
  return members.map((member: any) => ({
    id: String(member?.id || "").trim(),
    username: normalizeString(member?.username),
    email: normalizeString(member?.email),
    initials: normalizeString(member?.initials),
    color: normalizeString(member?.color),
    profilePicture: normalizeString(member?.profilePicture),
    name:
      normalizeString(member?.displayName) ||
      normalizeString(member?.username) ||
      normalizeString(member?.email) ||
      "Unassigned",
  }));
}

export async function uploadClickUpTaskAttachment(args: {
  taskId: string;
  fileName: string;
  buffer: Buffer;
  contentType?: string;
}) {
  const taskId = normalizeString(args.taskId);
  const fileName = normalizeString(args.fileName) || "attachment.bin";
  const token = getToken();

  if (!taskId) {
    const err: any = new Error(
      "taskId is required for ClickUp attachment upload",
    );
    err.status = 400;
    throw err;
  }

  if (!token) {
    throw new ClickUpConfigError(
      "ClickUp is not configured (missing token).",
      "CLICKUP_NOT_CONFIGURED",
    );
  }

  if (!args.buffer || !Buffer.isBuffer(args.buffer) || !args.buffer.length) {
    const err: any = new Error(
      "Attachment buffer is required for ClickUp upload",
    );
    err.status = 400;
    throw err;
  }

  const form = new FormData();
  const blob = new Blob([args.buffer], {
    type: args.contentType || "application/octet-stream",
  });

  form.append("attachment", blob, fileName);

  const res = await fetch(
    `${BASE}/task/${encodeURIComponent(taskId)}/attachment`,
    {
      method: "POST",
      headers: {
        Authorization: token,
      },
      body: form,
    },
  );

  const { raw, data } = await parseResponseBody(res);

  if (!res.ok) {
    const message = extractErrorMessage(data, res.status);

    const err: any = new Error(message);
    err.status = res.status;
    err.data = data;
    err.raw = raw;
    throw err;
  }

  return data;
}

export const clickupUploadAttachment = async (args: {
  taskId: string;
  fileName: string;
  buffer: Buffer;
  contentType?: string;
}) => uploadClickUpTaskAttachment(args);

export async function listClickUpTasks(args?: ListArgs) {
  const opts =
    typeof args === "string"
      ? { listId: args }
      : {
          listId: args?.listId,
          tag: args?.tag,
          clientId: args?.clientId,
        };

  const lid = String(opts?.listId || getDefaultListId() || "").trim();
  if (!lid) {
    throw new ClickUpConfigError(
      "ClickUp list is not configured (missing default list id).",
      "CLICKUP_NOT_CONFIGURED",
    );
  }

  const data = await cuFetch(
    `/list/${encodeURIComponent(lid)}/task?archived=false&include_closed=true&subtasks=true&page=0&include_timl=true`,
    { method: "GET" },
  );

  const tasks = Array.isArray(data?.tasks) ? data.tasks : [];
  const normalized = dedupeNormalizedTasks(tasks);

  const desiredTag = String(
    opts?.tag || (opts?.clientId ? `client:${opts.clientId}` : ""),
  ).trim();

  if (!desiredTag) return normalized;

  return normalized.filter((task: any) => {
    const tags = Array.isArray(task?.tags) ? task.tags : [];
    return tags.includes(desiredTag) || task.client_id === opts?.clientId;
  });
}

export async function getClickUpTask(taskId: string) {
  const id = normalizeString(taskId);
  if (!id) return null;

  const task = await cuFetch(`/task/${encodeURIComponent(id)}`, {
    method: "GET",
  });

  return normalizeTask(task);
}

export async function updateClickUpTaskStatus(args: {
  taskId: string;
  status: string;
}) {
  const taskId = normalizeString(args?.taskId);
  const status = normalizeString(args?.status);

  if (!taskId || !status) {
    const err: any = new Error("taskId and status are required");
    err.status = 400;
    throw err;
  }

  const updated = await cuFetch(`/task/${encodeURIComponent(taskId)}`, {
    method: "PUT",
    body: JSON.stringify({ status }),
  });

  return normalizeTask(updated);
}

export async function updateClickUpTask(args: UpdateArgs) {
  const taskId = normalizeString(args?.taskId);
  if (!taskId) {
    const err: any = new Error("taskId is required");
    err.status = 400;
    throw err;
  }

  const hasAssigneeIds = Array.isArray(args?.assigneeIds);
  const hasDueDateProp = Object.prototype.hasOwnProperty.call(
    args || {},
    "dueDate",
  );

  let currentTask: any = null;
  if (hasAssigneeIds) {
    try {
      currentTask = await cuFetch(`/task/${encodeURIComponent(taskId)}`, {
        method: "GET",
      });
    } catch {
      currentTask = null;
    }
  }

  const body: any = {};

  if (normalizeString(args?.title)) {
    body.name = normalizeString(args?.title);
  }

  if (args?.description !== undefined) {
    body.description = String(args.description ?? "");
  }

  if (normalizeString(args?.status)) {
    body.status = normalizeString(args?.status);
  }

  if (hasAssigneeIds) {
    const nextIds = (args.assigneeIds || [])
      .map((v) => (typeof v === "number" ? String(v) : normalizeString(v)))
      .filter((v) => v !== "");

    const currentIds = Array.isArray(currentTask?.assignees)
      ? currentTask.assignees
          .map((a: any) =>
            String(a?.id ?? a?.userid ?? a?.user_id ?? "").trim(),
          )
          .filter(Boolean)
      : [];

    const add = nextIds.filter((id) => !currentIds.includes(id));
    const rem = currentIds.filter((id) => !nextIds.includes(id));

    body.assignees = {
      add: add.map((id) => {
        const n = Number(id);
        return Number.isFinite(n) ? n : id;
      }),
      rem: rem.map((id) => {
        const n = Number(id);
        return Number.isFinite(n) ? n : id;
      }),
    };
  }

  if (hasDueDateProp) {
    if (args?.dueDate === null || args?.dueDate === "") {
      body.due_date = null;
    } else {
      const dueDate = toClickUpDueDate(args?.dueDate);
      if (dueDate !== undefined) {
        body.due_date = dueDate;
      }
    }
  }

  if (
    !Object.keys(body).length &&
    !normalizeString(args?.clientName || args?.clientId)
  ) {
    return getClickUpTask(taskId);
  }

  if (Object.keys(body).length) {
    await cuFetch(`/task/${encodeURIComponent(taskId)}`, {
      method: "PUT",
      body: JSON.stringify(body),
    });
  }

  const clientValue = normalizeString(args?.clientName || args?.clientId);
  if (clientValue) {
    await setTaskClientFields({
      taskId,
      value: clientValue,
    }).catch(() => null);
  }

  const refreshed = await cuFetch(`/task/${encodeURIComponent(taskId)}`, {
    method: "GET",
  });

  return normalizeTask(refreshed);
}

export async function deleteClickUpTask(taskId: string) {
  const id = normalizeString(taskId);
  if (!id) {
    const err: any = new Error("taskId is required");
    err.status = 400;
    throw err;
  }

  await cuFetch(`/task/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });

  return { ok: true, id };
}

export async function createClickUpTask(payload: CreateArgs) {
  const lid = String(payload?.listId || getDefaultListId() || "").trim();
  if (!lid) {
    throw new ClickUpConfigError(
      "ClickUp list is not configured (missing default list id).",
      "CLICKUP_NOT_CONFIGURED",
    );
  }

  const tag = String(
    payload?.tag || (payload?.clientId ? `client:${payload.clientId}` : ""),
  ).trim();

  const clientFieldValue = String(
    payload?.clientName || payload?.clientId || "",
  ).trim();

  const body: any = {
    name: String(payload.title || "").trim(),
    description: String(payload.description || ""),
  };

  if (!body.name) {
    const err: any = new Error("Task title is required");
    err.status = 400;
    throw err;
  }

  if (tag) {
    body.tags = [tag];
  }

  if (Array.isArray(payload?.assigneeIds) && payload.assigneeIds.length) {
    body.assignees = payload.assigneeIds
      .map((v) => (typeof v === "number" ? v : normalizeString(v)))
      .filter((v) => String(v).trim() !== "");
  }

  const created = await cuFetch(`/list/${encodeURIComponent(lid)}/task`, {
    method: "POST",
    body: JSON.stringify(body),
  });

  if (created?.id && clientFieldValue) {
    await setTaskClientFields({
      taskId: created.id,
      listId: lid,
      value: clientFieldValue,
    });
  }

  const refreshed = created?.id
    ? await cuFetch(`/task/${encodeURIComponent(created.id)}`, {
        method: "GET",
      }).catch(() => created)
    : created;

  const refreshedTags = extractTags(refreshed);
  const refreshedClientField = extractClientValue(refreshed);
  const refreshedClientTag =
    refreshedTags
      .find((x) => x.startsWith("client:"))
      ?.replace(/^client:/, "") || "";
  const refreshedDueDate = toClickUpDueDate(refreshed?.due_date);

  return {
    id: refreshed?.id,
    name: refreshed?.name,
    title: refreshed?.name,
    description: refreshed?.description || "",
    status: refreshed?.status?.status || refreshed?.status || "",
    url: refreshed?.url,
    tags: refreshedTags,
    assignee: extractAssignee(refreshed),
    assignees: Array.isArray(refreshed?.assignees) ? refreshed.assignees : [],
    assignee_ids: extractAssigneeIds(refreshed),
    client_id:
      refreshedClientTag || payload?.clientId || refreshedClientField || "",
    client_name:
      refreshedClientField ||
      payload?.clientName ||
      refreshedClientTag ||
      payload?.clientId ||
      "",
    clickup_task_id: refreshed?.id || null,
    source: "clickup",
    due_date: refreshedDueDate
      ? new Date(Number(refreshedDueDate)).toISOString()
      : null,
    updated_at: refreshed?.date_updated || null,
  };
}

/** Legacy aliases expected by some routes */
export const clickupListTasks = async (
  args?: string | { listId?: string; tag?: string; clientId?: string },
) => listClickUpTasks(args);

export const clickupCreateTask = async (args: {
  title: string;
  description?: string;
  listId?: string;
  tag?: string;
  clientId?: string;
  clientName?: string;
  assigneeIds?: (string | number)[];
}) => createClickUpTask(args);

export function buildClickUpTaskUrl(taskId: string) {
  const normalized = String(taskId || "").trim();
  return normalized
    ? `https://app.clickup.com/t/${encodeURIComponent(normalized)}`
    : "";
}
