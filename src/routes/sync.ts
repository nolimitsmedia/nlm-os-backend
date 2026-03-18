// services/api/src/routes/sync.ts
import { Router } from "express";
import { query } from "../db.js";
import { getAuthUserFromReq, type AuthUser } from "../middleware/auth.js";
import {
  getWhmcsAutoSyncConfig,
  getWhmcsAutoSyncState,
  runWhmcsSyncOnce,
} from "../jobs/whmcsSync.js";

const router = Router();

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function hasWhmcsConfig() {
  const hasModernPair = Boolean(
    env("WHMCS_API_URL") &&
    env("WHMCS_API_IDENTIFIER") &&
    env("WHMCS_API_SECRET"),
  );

  const hasLegacyPair = Boolean(
    env("WHMCS_API_URL") &&
    env("WHMCS_API_USERNAME") &&
    env("WHMCS_API_ACCESS_KEY"),
  );

  return hasModernPair || hasLegacyPair;
}

function parseBool(value: any, fallback = false) {
  const v = String(value ?? "")
    .trim()
    .toLowerCase();
  if (!v) return fallback;
  return ["1", "true", "yes", "on"].includes(v);
}

function hasAllowedRole(user: AuthUser | null) {
  if (!user) return false;
  const role = String(user.role || "")
    .trim()
    .toLowerCase();
  return ["admin", "staff"].includes(role);
}

function isAuthorized(req: any) {
  const requiredToken = env("SYNC_ADMIN_TOKEN");
  const authHeader = String(req.headers.authorization || "");
  const bearer = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7).trim()
    : "";

  const syncToken =
    String(req.headers["x-sync-token"] || "") ||
    String(req.query.token || "") ||
    String(req.body?.token || "") ||
    "";

  if (requiredToken) {
    if (syncToken && syncToken === requiredToken) return true;
    if (bearer && bearer === requiredToken) return true;
  }

  const user = getAuthUserFromReq(req);
  return hasAllowedRole(user);
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

async function buildSyncRunsOrderExpr() {
  const hasFinishedAt = await columnExists("sync_runs", "finished_at");
  const hasStartedAt = await columnExists("sync_runs", "started_at");
  const hasCreatedAt = await columnExists("sync_runs", "created_at");

  const cols: string[] = [];
  if (hasFinishedAt) cols.push("finished_at");
  if (hasStartedAt) cols.push("started_at");
  if (hasCreatedAt) cols.push("created_at");

  if (!cols.length) return "now()";
  if (cols.length === 1) return cols[0];
  return `COALESCE(${cols.join(", ")}, now())`;
}

// GET /sync/health
router.get("/health", async (_req, res) => {
  try {
    const config = {
      whmcs_configured: hasWhmcsConfig(),
      whmcs_api_url: env("WHMCS_API_URL") || null,
      whmcs_sync_enabled: parseBool(env("WHMCS_SYNC_ENABLED", "true"), true),
      whmcs_sync_page_size: Number(env("WHMCS_SYNC_PAGE_SIZE", "250")),
      whmcs_sync_timeout_ms: Number(env("WHMCS_SYNC_TIMEOUT_MS", "30000")),
      whmcs_sync_client_status: env("WHMCS_SYNC_CLIENT_STATUS") || null,
      whmcs_auth_mode:
        env("WHMCS_API_IDENTIFIER") && env("WHMCS_API_SECRET")
          ? "identifier_secret"
          : env("WHMCS_API_USERNAME") && env("WHMCS_API_ACCESS_KEY")
            ? "username_access_key"
            : null,
      ...getWhmcsAutoSyncConfig(),
    };

    let lastRun: any = null;

    try {
      const orderExpr = await buildSyncRunsOrderExpr();
      const r = await query(
        `
        SELECT *
        FROM sync_runs
        WHERE source = 'whmcs'
        ORDER BY ${orderExpr} DESC
        LIMIT 1
        `,
      );
      lastRun = r.rows?.[0] ?? null;
    } catch {
      lastRun = null;
    }

    res.json({
      ok: true,
      config,
      autoSync: getWhmcsAutoSyncState(),
      lastRun,
    });
  } catch (e: any) {
    res.status(500).json({
      ok: false,
      error: e?.message || "failed to load sync health",
    });
  }
});

// GET /sync/whmcs/status
router.get("/whmcs/status", async (_req, res) => {
  try {
    const orderExpr = await buildSyncRunsOrderExpr();

    const runs = await query(
      `
      SELECT *
      FROM sync_runs
      WHERE source = 'whmcs'
      ORDER BY ${orderExpr} DESC
      LIMIT 10
      `,
    );

    const counts = {
      clients: 0,
      invoices: 0,
      services: 0,
    };

    try {
      const c1 = await query<{ n: number }>(
        `SELECT COUNT(*)::int AS n FROM whmcs_clients_cache`,
      );
      counts.clients = c1.rows?.[0]?.n ?? 0;
    } catch {}

    try {
      const c2 = await query<{ n: number }>(
        `SELECT COUNT(*)::int AS n FROM whmcs_invoices_cache`,
      );
      counts.invoices = c2.rows?.[0]?.n ?? 0;
    } catch {}

    try {
      const c3 = await query<{ n: number }>(
        `SELECT COUNT(*)::int AS n FROM whmcs_services_cache`,
      );
      counts.services = c3.rows?.[0]?.n ?? 0;
    } catch {}

    res.json({
      ok: true,
      configured: hasWhmcsConfig(),
      counts,
      runs: runs.rows ?? [],
      autoSync: getWhmcsAutoSyncState(),
    });
  } catch (e: any) {
    res.status(500).json({
      ok: false,
      error: e?.message || "failed to load WHMCS sync status",
    });
  }
});

// POST /sync/whmcs/run
router.post("/whmcs/run", async (req, res) => {
  try {
    if (!isAuthorized(req)) {
      return res.status(401).json({
        ok: false,
        error: "unauthorized",
      });
    }

    if (!hasWhmcsConfig()) {
      return res.status(400).json({
        ok: false,
        error:
          "WHMCS is not configured. Provide WHMCS_API_URL plus either WHMCS_API_IDENTIFIER/WHMCS_API_SECRET or WHMCS_API_USERNAME/WHMCS_API_ACCESS_KEY.",
      });
    }

    const result = await runWhmcsSyncOnce({
      trigger: "manual",
      initiatedBy:
        (req.user && String(req.user.email || req.user.id || "")) ||
        "dashboard",
    });

    return res.json({
      ok: true,
      result,
    });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "sync failed",
      stack:
        process.env.NODE_ENV !== "production"
          ? String(e?.stack || "")
          : undefined,
    });
  }
});

export default router;
