// services/api/src/jobs/whmcsSync.ts
import { query } from "../db.js";

type RunRow = { id: string };

function firstEnv(names: string[], optional = false) {
  for (const name of names) {
    const value = String(process.env[name] ?? "").trim();
    if (value) return value;
  }
  if (!optional) {
    throw new Error(`${names[0]} is not set (services/api/.env)`);
  }
  return "";
}

function parseWhmcsBlockedIp(message: string) {
  const match = String(message || "").match(/Invalid IP\s+([0-9.]+)/i);
  return match?.[1] || null;
}

function buildWhmcsAuthFields() {
  const identifier = firstEnv(["WHMCS_API_IDENTIFIER"], true);
  const secret = firstEnv(["WHMCS_API_SECRET"], true);

  if (identifier && secret) {
    return {
      mode: "identifier-secret" as const,
      fields: {
        identifier,
        secret,
      },
    };
  }

  const username = firstEnv(
    ["WHMCS_API_USERNAME", "WHMCS_ADMIN_USERNAME"],
    true,
  );
  const accessKey = firstEnv(
    ["WHMCS_API_ACCESS_KEY", "WHMCS_ACCESS_KEY"],
    true,
  );

  if (username && accessKey) {
    return {
      mode: "username-accesskey" as const,
      fields: {
        username,
        accesskey: accessKey,
      },
    };
  }

  throw new Error(
    "WHMCS credentials are not set. Provide WHMCS_API_IDENTIFIER + WHMCS_API_SECRET or WHMCS_API_USERNAME + WHMCS_API_ACCESS_KEY.",
  );
}

function normalizeWhmcsError(
  action: string,
  status: number,
  payload: any,
  rawText: string,
) {
  const payloadText = payload
    ? JSON.stringify(payload).slice(0, 700)
    : String(rawText || "").slice(0, 700);
  const invalidIp = parseWhmcsBlockedIp(payloadText);

  if (status === 403 && invalidIp) {
    return new Error(
      `WHMCS blocked this server IP (${invalidIp}) for action ${action}. Check the exact API credential pair in WHMCS API IP Access Restriction, then verify Trusted Proxies if WHMCS is behind a proxy/load balancer. Raw WHMCS response: ${payloadText}`,
    );
  }

  return new Error(`WHMCS HTTP ${status}: ${payloadText}`);
}

function env(name: string, optional = false) {
  const v = process.env[name];
  if (!v && !optional) {
    throw new Error(`${name} is not set (services/api/.env)`);
  }
  return String(v || "").trim();
}

function envBool(name: string, fallback = false) {
  const v = String(process.env[name] ?? "")
    .trim()
    .toLowerCase();
  if (!v) return fallback;
  return ["1", "true", "yes", "on"].includes(v);
}

function envNum(name: string, fallback: number) {
  const n = Number(process.env[name] ?? fallback);
  return Number.isFinite(n) ? n : fallback;
}

async function columnExists(table: string, column: string) {
  const r = await query<{ exists: boolean }>(
    `SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name=$1 AND column_name=$2
    ) AS "exists"`,
    [table, column],
  );
  return !!r.rows?.[0]?.exists;
}

async function constraintExists(name: string) {
  const r = await query<{ exists: boolean }>(
    `SELECT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conname = $1
    ) AS "exists"`,
    [name],
  );
  return !!r.rows?.[0]?.exists;
}

async function startRun(kind: string, source: string) {
  const now = new Date();

  const hasSource = await columnExists("sync_runs", "source");
  const hasKind = await columnExists("sync_runs", "kind");
  const hasStatus = await columnExists("sync_runs", "status");
  const hasStartedAt = await columnExists("sync_runs", "started_at");

  const cols: string[] = [];
  const vals: any[] = [];

  if (hasSource) {
    vals.push(source);
    cols.push("source");
  }
  if (hasKind) {
    vals.push(kind);
    cols.push("kind");
  }
  if (hasStatus) {
    vals.push("running");
    cols.push("status");
  }
  if (hasStartedAt) {
    vals.push(now);
    cols.push("started_at");
  }

  const sql =
    cols.length > 0
      ? `INSERT INTO sync_runs (${cols.join(", ")})
         VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")})
         RETURNING id::text AS id`
      : `INSERT INTO sync_runs DEFAULT VALUES RETURNING id::text AS id`;

  const r = await query<RunRow>(sql, vals);
  return r.rows[0].id;
}

async function finishRun(runId: string, ok: boolean, stats: any, error?: any) {
  const hasFinishedAt = await columnExists("sync_runs", "finished_at");
  const hasStatus = await columnExists("sync_runs", "status");
  const hasStats = await columnExists("sync_runs", "stats");
  const hasError = await columnExists("sync_runs", "error_message");

  const sets: string[] = [];
  const vals: any[] = [];

  if (hasFinishedAt) {
    vals.push(new Date());
    sets.push(`finished_at=$${vals.length}`);
  }
  if (hasStatus) {
    vals.push(ok ? "success" : "error");
    sets.push(`status=$${vals.length}`);
  }
  if (hasStats) {
    vals.push(stats ?? {});
    sets.push(`stats=$${vals.length}`);
  }
  if (!ok && hasError) {
    vals.push(String(error?.message || error || "unknown error"));
    sets.push(`error_message=$${vals.length}`);
  }

  vals.push(runId);
  const sql =
    sets.length > 0
      ? `UPDATE sync_runs SET ${sets.join(", ")} WHERE id=$${vals.length}`
      : `UPDATE sync_runs SET id=id WHERE id=$${vals.length}`;

  await query(sql, vals);
}

/* ───────────────────────── schema ensure ───────────────────────── */

async function ensureClientsCacheTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS whmcs_clients_cache (
      whmcs_client_id  INTEGER PRIMARY KEY,
      email            TEXT,
      first_name       TEXT,
      last_name        TEXT,
      company_name     TEXT,
      status           TEXT,
      currency         TEXT,
      date_created     TIMESTAMPTZ,
      raw              JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_synced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await query(`
    ALTER TABLE whmcs_clients_cache
    ADD COLUMN IF NOT EXISTS email TEXT,
    ADD COLUMN IF NOT EXISTS first_name TEXT,
    ADD COLUMN IF NOT EXISTS last_name TEXT,
    ADD COLUMN IF NOT EXISTS company_name TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS currency TEXT,
    ADD COLUMN IF NOT EXISTS date_created TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now();
  `);
}

async function ensureInvoicesCacheTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS whmcs_invoices_cache (
      invoice_id       INTEGER PRIMARY KEY,
      whmcs_client_id  INTEGER NOT NULL,
      status           TEXT NOT NULL,
      total            NUMERIC(12,2) NOT NULL DEFAULT 0,
      balance          NUMERIC(12,2) NOT NULL DEFAULT 0,
      date_created     TIMESTAMPTZ,
      date_due         TIMESTAMPTZ,
      date_paid        TIMESTAMPTZ,
      raw              JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_synced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await query(`
    ALTER TABLE whmcs_invoices_cache
    ADD COLUMN IF NOT EXISTS invoice_id INTEGER,
    ADD COLUMN IF NOT EXISTS whmcs_client_id INTEGER,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS total NUMERIC(12,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS balance NUMERIC(12,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS date_created TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS date_due TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS date_paid TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now();
  `);

  const hasPrimaryKey = await constraintExists("whmcs_invoices_cache_pkey");
  const hasInvoiceId = await columnExists("whmcs_invoices_cache", "invoice_id");

  if (!hasPrimaryKey && hasInvoiceId) {
    try {
      await query(`
        ALTER TABLE whmcs_invoices_cache
        ADD CONSTRAINT whmcs_invoices_cache_pkey PRIMARY KEY (invoice_id);
      `);
    } catch {
      // Legacy duplicates or bad rows can block PK creation. Safe to ignore for now.
    }
  }

  await query(
    `CREATE INDEX IF NOT EXISTS idx_invoices_client ON whmcs_invoices_cache(whmcs_client_id);`,
  );
  await query(
    `CREATE INDEX IF NOT EXISTS idx_invoices_status ON whmcs_invoices_cache(status);`,
  );
}

async function ensureServicesCacheTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS whmcs_services_cache (
      service_id       INTEGER PRIMARY KEY,
      whmcs_client_id  INTEGER NOT NULL,
      status           TEXT NOT NULL,
      recurring_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      billing_cycle    TEXT,
      product_name     TEXT,
      domain           TEXT,
      next_due_date    TIMESTAMPTZ,
      raw              JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_synced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await query(`
    ALTER TABLE whmcs_services_cache
    ADD COLUMN IF NOT EXISTS service_id INTEGER,
    ADD COLUMN IF NOT EXISTS whmcs_client_id INTEGER,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS recurring_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS billing_cycle TEXT,
    ADD COLUMN IF NOT EXISTS product_name TEXT,
    ADD COLUMN IF NOT EXISTS domain TEXT,
    ADD COLUMN IF NOT EXISTS next_due_date TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now();
  `);

  const hasPrimaryKey = await constraintExists("whmcs_services_cache_pkey");
  const hasServiceId = await columnExists("whmcs_services_cache", "service_id");

  if (!hasPrimaryKey && hasServiceId) {
    try {
      await query(`
        ALTER TABLE whmcs_services_cache
        ADD CONSTRAINT whmcs_services_cache_pkey PRIMARY KEY (service_id);
      `);
    } catch {
      // Legacy duplicates or bad rows can block PK creation. Safe to ignore for now.
    }
  }

  await query(
    `CREATE INDEX IF NOT EXISTS idx_services_client ON whmcs_services_cache(whmcs_client_id);`,
  );
  await query(
    `CREATE INDEX IF NOT EXISTS idx_services_status ON whmcs_services_cache(status);`,
  );
}

async function ensureTables() {
  await ensureClientsCacheTable();
  await ensureInvoicesCacheTable();
  await ensureServicesCacheTable();
}

function toIsoOrNull(v: any) {
  if (!v) return null;

  const s = String(v).trim();
  if (!s || s === "0000-00-00" || s === "0000-00-00 00:00:00") {
    return null;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const d = new Date(`${s}T00:00:00.000Z`);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }

  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function num(v: any) {
  const n = Number(String(v ?? "").replace(/[^0-9.\-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function normalizeStatus(v: any) {
  const s = String(v ?? "").trim();
  return s || null;
}

function isUtf8JsonErrorMessage(message: string) {
  const s = String(message || "").toLowerCase();
  return (
    s.includes("malformed utf-8") ||
    s.includes("incorrectly encoded") ||
    s.includes("error generating json encoded response")
  );
}

/* ───────────────────────── WHMCS API ─────────────────────────
   Required .env:
     WHMCS_API_URL=https://yourdomain.com/includes/api.php
     WHMCS_API_IDENTIFIER=xxxx
     WHMCS_API_SECRET=xxxx

   Optional:
     WHMCS_SYNC_ENABLED=true
     WHMCS_SYNC_TIMEOUT_MS=30000
     WHMCS_SYNC_PAGE_SIZE=250
     WHMCS_SYNC_CLIENT_STATUS=Active
*/

async function whmcsCall<T = any>(action: string, params: Record<string, any>) {
  const url = env("WHMCS_API_URL");
  const timeoutMs = envNum("WHMCS_SYNC_TIMEOUT_MS", 30000);
  const auth = buildWhmcsAuthFields();

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const body = new URLSearchParams();
  body.set("action", action);
  body.set("responsetype", "json");

  for (const [k, v] of Object.entries(auth.fields)) {
    if (v === undefined || v === null || v === "") continue;
    body.set(k, String(v));
  }

  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null || v === "") continue;
    body.set(k, String(v));
  }

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json, text/plain, */*",
        "User-Agent": "NLM-OS-WHMCS-Sync/1.0",
        "X-Requested-With": "XMLHttpRequest",
      },
      body,
      signal: controller.signal,
    });

    const text = await res.text();
    let json: any;

    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      throw new Error(
        `WHMCS response not JSON (${res.status}) for action ${action}: ${text.slice(0, 400)}`,
      );
    }

    if (!res.ok) {
      throw normalizeWhmcsError(action, res.status, json, text);
    }

    if (json?.result && String(json.result).toLowerCase() === "error") {
      const message = String(json?.message || "unknown");
      const invalidIp = parseWhmcsBlockedIp(message);
      if (invalidIp) {
        throw new Error(
          `WHMCS blocked this server IP (${invalidIp}) for action ${action}. Check the exact API credential pair in WHMCS API IP Access Restriction, then verify Trusted Proxies if WHMCS is behind a proxy/load balancer. Raw WHMCS response: ${JSON.stringify(json).slice(0, 700)}`,
        );
      }
      throw new Error(`WHMCS error [${action}]: ${message}`);
    }

    return json as T;
  } catch (error: any) {
    if (error?.name === "AbortError") {
      throw new Error(
        `WHMCS request timeout after ${timeoutMs}ms for action ${action}`,
      );
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchAllClients() {
  const limitnum = envNum("WHMCS_SYNC_PAGE_SIZE", 250);
  const statusFilter = env("WHMCS_SYNC_CLIENT_STATUS", true);
  let limitstart = 0;
  const all: any[] = [];

  while (true) {
    const r: any = await whmcsCall("GetClients", {
      limitstart,
      limitnum,
      sorting: "ASC",
      ...(statusFilter ? { status: statusFilter } : {}),
    });

    const clients = r?.clients?.client;
    const arr = Array.isArray(clients) ? clients : clients ? [clients] : [];
    all.push(...arr);

    const total = Number(r?.totalresults ?? all.length);
    limitstart += limitnum;

    if (all.length >= total || arr.length === 0) break;
  }

  return all;
}

async function fetchInvoicesForClient(clientId: number) {
  const limitnum = envNum("WHMCS_SYNC_PAGE_SIZE", 250);
  let limitstart = 0;
  const all: any[] = [];

  while (true) {
    const r: any = await whmcsCall("GetInvoices", {
      userid: clientId,
      limitstart,
      limitnum,
      orderby: "id",
      order: "ASC",
    });

    const invoices = r?.invoices?.invoice;
    const arr = Array.isArray(invoices) ? invoices : invoices ? [invoices] : [];
    all.push(...arr);

    const total = Number(r?.totalresults ?? all.length);
    limitstart += limitnum;

    if (all.length >= total || arr.length === 0) break;
  }

  return all;
}

async function fetchServicesForClient(clientId: number) {
  const limitnum = envNum("WHMCS_SYNC_PAGE_SIZE", 250);
  let limitstart = 0;
  const all: any[] = [];

  while (true) {
    const r: any = await whmcsCall("GetClientsProducts", {
      clientid: clientId,
      limitstart,
      limitnum,
      stats: 0,
    });

    const products = r?.products?.product;
    const arr = Array.isArray(products) ? products : products ? [products] : [];
    all.push(...arr);

    const total = Number(r?.totalresults ?? all.length);
    limitstart += limitnum;

    if (all.length >= total || arr.length === 0) break;
  }

  return all;
}

/* ───────────────────────── upserts ───────────────────────── */

async function upsertClientCache(row: any) {
  const id = Number(row?.id);
  if (!Number.isFinite(id) || id <= 0) return;

  const first = row?.firstname ?? row?.first_name ?? null;
  const last = row?.lastname ?? row?.last_name ?? null;
  const company = row?.companyname ?? row?.company_name ?? null;
  const email = row?.email ?? null;
  const status = normalizeStatus(row?.status);
  const currency = row?.currency ?? null;
  const dateCreated = toIsoOrNull(row?.datecreated ?? row?.date_created);

  await query(
    `
    INSERT INTO whmcs_clients_cache
      (whmcs_client_id, email, first_name, last_name, company_name, status, currency, date_created, raw, last_synced_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, now())
    ON CONFLICT (whmcs_client_id) DO UPDATE SET
      email=EXCLUDED.email,
      first_name=EXCLUDED.first_name,
      last_name=EXCLUDED.last_name,
      company_name=EXCLUDED.company_name,
      status=EXCLUDED.status,
      currency=EXCLUDED.currency,
      date_created=EXCLUDED.date_created,
      raw=EXCLUDED.raw,
      last_synced_at=now()
    `,
    [id, email, first, last, company, status, currency, dateCreated, row],
  );
}

async function syncLinkedLocalClientStatus(row: any) {
  const whmcsClientId = Number(row?.id);
  if (!Number.isFinite(whmcsClientId) || whmcsClientId <= 0) return;

  const normalizedStatus = String(normalizeStatus(row?.status) || "")
    .trim()
    .toLowerCase();

  if (!normalizedStatus) return;

  await query(
    `
    UPDATE clients
    SET status = $2
    WHERE whmcs_client_id = $1
      AND COALESCE(NULLIF(TRIM(status), ''), '') <> $2
    `,
    [whmcsClientId, normalizedStatus],
  ).catch(() => {});
}

async function upsertInvoiceCache(whmcsClientId: number, row: any) {
  const invoiceId = Number(row?.id ?? row?.invoiceid ?? row?.invoice_id);
  if (!Number.isFinite(invoiceId) || invoiceId <= 0) return;

  const status = String(row?.status ?? "unknown");
  const total = num(row?.total);
  const balance = num(row?.balance ?? row?.amountdue ?? row?.amount_due);
  const dateCreated = toIsoOrNull(row?.date ?? row?.date_created);
  const dateDue = toIsoOrNull(row?.duedate ?? row?.date_due);
  const datePaid = toIsoOrNull(row?.datepaid ?? row?.date_paid);

  await query(
    `
    INSERT INTO whmcs_invoices_cache
      (invoice_id, whmcs_client_id, status, total, balance, date_created, date_due, date_paid, raw, last_synced_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, now())
    ON CONFLICT (invoice_id) DO UPDATE SET
      whmcs_client_id=EXCLUDED.whmcs_client_id,
      status=EXCLUDED.status,
      total=EXCLUDED.total,
      balance=EXCLUDED.balance,
      date_created=EXCLUDED.date_created,
      date_due=EXCLUDED.date_due,
      date_paid=EXCLUDED.date_paid,
      raw=EXCLUDED.raw,
      last_synced_at=now()
    `,
    [
      invoiceId,
      whmcsClientId,
      status,
      total,
      balance,
      dateCreated,
      dateDue,
      datePaid,
      row,
    ],
  );
}

async function upsertServiceCache(whmcsClientId: number, row: any) {
  const serviceId = Number(row?.id ?? row?.serviceid ?? row?.service_id);
  if (!Number.isFinite(serviceId) || serviceId <= 0) return;

  const status = String(row?.status ?? "unknown");
  const recurring = num(
    row?.recurringamount ?? row?.recurring_amount ?? row?.amount,
  );
  const billingCycle = row?.billingcycle ?? row?.billing_cycle ?? null;
  const productName =
    row?.productname ??
    row?.product_name ??
    row?.name ??
    row?.package_name ??
    null;
  const domain = row?.domain ?? null;
  const nextDue = toIsoOrNull(row?.nextduedate ?? row?.next_due_date);

  await query(
    `
    INSERT INTO whmcs_services_cache
      (service_id, whmcs_client_id, status, recurring_amount, billing_cycle, product_name, domain, next_due_date, raw, last_synced_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, now())
    ON CONFLICT (service_id) DO UPDATE SET
      whmcs_client_id=EXCLUDED.whmcs_client_id,
      status=EXCLUDED.status,
      recurring_amount=EXCLUDED.recurring_amount,
      billing_cycle=EXCLUDED.billing_cycle,
      product_name=EXCLUDED.product_name,
      domain=EXCLUDED.domain,
      next_due_date=EXCLUDED.next_due_date,
      raw=EXCLUDED.raw,
      last_synced_at=now()
    `,
    [
      serviceId,
      whmcsClientId,
      status,
      recurring,
      billingCycle,
      productName,
      domain,
      nextDue,
      row,
    ],
  );
}

async function cleanupStaleCaches(activeClientIds: number[]) {
  if (!activeClientIds.length) return;

  await query(
    `
    DELETE FROM whmcs_invoices_cache
    WHERE whmcs_client_id NOT IN (
      SELECT UNNEST($1::int[])
    )
    `,
    [activeClientIds],
  );

  await query(
    `
    DELETE FROM whmcs_services_cache
    WHERE whmcs_client_id NOT IN (
      SELECT UNNEST($1::int[])
    )
    `,
    [activeClientIds],
  );

  await query(
    `
    DELETE FROM whmcs_clients_cache
    WHERE whmcs_client_id NOT IN (
      SELECT UNNEST($1::int[])
    )
    `,
    [activeClientIds],
  );
}

/* ───────────────────────── main job ───────────────────────── */

type WhmcsRunOptions = {
  trigger?: "manual" | "auto" | "startup";
  initiatedBy?: string | null;
};

function autoSyncEnabled() {
  return envBool("WHMCS_AUTO_SYNC_ENABLED", true);
}

function autoSyncIntervalMs() {
  return Math.max(60_000, envNum("WHMCS_AUTO_SYNC_INTERVAL_MS", 5 * 60_000));
}

function autoSyncRunOnStartup() {
  return envBool("WHMCS_AUTO_SYNC_RUN_ON_STARTUP", true);
}

const AUTO_SYNC_STATE = {
  enabled: false,
  intervalMs: 0,
  timerStarted: false,
  running: false,
  lastAttemptAt: null as string | null,
  lastSuccessAt: null as string | null,
  lastError: null as string | null,
};

let whmcsSyncTimer: ReturnType<typeof setInterval> | null = null;
let currentRunPromise: Promise<any> | null = null;

export function getWhmcsAutoSyncConfig() {
  return {
    whmcs_auto_sync_enabled: autoSyncEnabled(),
    whmcs_auto_sync_interval_ms: autoSyncIntervalMs(),
    whmcs_auto_sync_run_on_startup: autoSyncRunOnStartup(),
  };
}

export function getWhmcsAutoSyncState() {
  return {
    ...AUTO_SYNC_STATE,
  };
}

export async function runWhmcsSyncOnce(options: WhmcsRunOptions = {}) {
  if (currentRunPromise) {
    return currentRunPromise;
  }

  currentRunPromise = (async () => {
    if (!envBool("WHMCS_SYNC_ENABLED", true)) {
      return {
        ok: true,
        skipped: true,
        reason: "WHMCS_SYNC_ENABLED is false",
      };
    }

    const kind = "whmcs_full_sync";
    const source = "whmcs";

    await ensureTables();

    const runId = await startRun(kind, source);
    const stats: any = {
      kind,
      source,
      trigger: options.trigger || "manual",
      initiated_by: options.initiatedBy || null,
      clients_seen: 0,
      clients_upserted: 0,
      invoices_upserted: 0,
      services_upserted: 0,
      services_failed_clients: 0,
      services_failed_client_ids: [] as number[],
      services_failed_reasons: [] as Array<{
        client_id: number;
        error: string;
      }>,
      cache_clients: 0,
      cache_invoices: 0,
      cache_services: 0,
    };

    AUTO_SYNC_STATE.running = true;
    AUTO_SYNC_STATE.lastAttemptAt = new Date().toISOString();
    AUTO_SYNC_STATE.lastError = null;

    try {
      const clients = await fetchAllClients();
      stats.clients_seen = clients.length;

      const activeClientIds: number[] = [];

      for (const c of clients) {
        const whmcsId = Number(c?.id);
        if (!Number.isFinite(whmcsId) || whmcsId <= 0) continue;

        activeClientIds.push(whmcsId);

        await upsertClientCache(c);
        await syncLinkedLocalClientStatus(c);
        stats.clients_upserted++;

        const invoices = await fetchInvoicesForClient(whmcsId);
        for (const inv of invoices) {
          await upsertInvoiceCache(whmcsId, inv);
          stats.invoices_upserted++;
        }

        try {
          const services = await fetchServicesForClient(whmcsId);
          for (const svc of services) {
            await upsertServiceCache(whmcsId, svc);
            stats.services_upserted++;
          }
        } catch (e: any) {
          const message = String(e?.message || e || "unknown error");

          if (isUtf8JsonErrorMessage(message)) {
            stats.services_failed_clients++;
            stats.services_failed_client_ids.push(whmcsId);
            stats.services_failed_reasons.push({
              client_id: whmcsId,
              error: message,
            });

            console.warn(
              `[whmcs-sync] skipping services for client ${whmcsId} due to malformed WHMCS UTF-8 data`,
            );
            continue;
          }

          throw e;
        }
      }

      await cleanupStaleCaches(activeClientIds);

      const cc = await query<{ n: number }>(
        `SELECT COUNT(*)::int AS n FROM whmcs_clients_cache`,
      );
      const ic = await query<{ n: number }>(
        `SELECT COUNT(*)::int AS n FROM whmcs_invoices_cache`,
      );
      const sc = await query<{ n: number }>(
        `SELECT COUNT(*)::int AS n FROM whmcs_services_cache`,
      );

      stats.cache_clients = cc.rows?.[0]?.n ?? 0;
      stats.cache_invoices = ic.rows?.[0]?.n ?? 0;
      stats.cache_services = sc.rows?.[0]?.n ?? 0;

      await finishRun(runId, true, stats);
      AUTO_SYNC_STATE.lastSuccessAt = new Date().toISOString();

      return { ok: true, runId, stats };
    } catch (e: any) {
      AUTO_SYNC_STATE.lastError = String(e?.message || e || "sync failed");
      await finishRun(runId, false, stats, e);
      throw e;
    } finally {
      AUTO_SYNC_STATE.running = false;
    }
  })();

  try {
    return await currentRunPromise;
  } finally {
    currentRunPromise = null;
  }
}

export async function runWhmcsSync() {
  return runWhmcsSyncOnce({ trigger: "manual", initiatedBy: "server" });
}

export function startWhmcsAutoSync() {
  if (whmcsSyncTimer) {
    return getWhmcsAutoSyncState();
  }

  const enabled = autoSyncEnabled();
  const intervalMs = autoSyncIntervalMs();

  AUTO_SYNC_STATE.enabled = enabled;
  AUTO_SYNC_STATE.intervalMs = intervalMs;

  if (!enabled) {
    console.log("[whmcs-sync] auto sync disabled");
    return getWhmcsAutoSyncState();
  }

  const tick = async (trigger: "auto" | "startup") => {
    try {
      await runWhmcsSyncOnce({
        trigger,
        initiatedBy:
          trigger === "startup" ? "server-startup" : "auto-sync-timer",
      });
    } catch (e: any) {
      AUTO_SYNC_STATE.lastError = String(e?.message || e || "sync failed");
      console.error(`[whmcs-sync] ${trigger} run failed:`, e);
    }
  };

  whmcsSyncTimer = setInterval(() => {
    void tick("auto");
  }, intervalMs);

  AUTO_SYNC_STATE.timerStarted = true;

  console.log(`[whmcs-sync] auto sync started (${intervalMs}ms interval)`);

  if (autoSyncRunOnStartup()) {
    setTimeout(() => {
      void tick("startup");
    }, 5_000);
  }

  return getWhmcsAutoSyncState();
}
