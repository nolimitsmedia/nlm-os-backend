// services/api/src/services/aiService.ts
import crypto from "crypto";
import { query } from "../db.js";

type CacheablePayload = Record<string, any>;

function stableStringify(value: any): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  const keys = Object.keys(value).sort();
  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(",")}}`;
}

export async function ensureAiSupportTables() {
  await query(`
    CREATE TABLE IF NOT EXISTS ai_logs (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id text,
      user_id text,
      prompt text NOT NULL,
      response text,
      provider text,
      model text,
      cache_key text,
      cached boolean DEFAULT false,
      route_reason text,
      request_id text,
      meta jsonb DEFAULT '{}'::jsonb,
      created_at timestamptz DEFAULT now()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS ai_cache (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      cache_key text UNIQUE NOT NULL,
      payload jsonb NOT NULL,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_ai_logs_created_at
    ON ai_logs(created_at DESC)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_ai_logs_client_id
    ON ai_logs(client_id)
  `).catch(() => null);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_ai_cache_created_at
    ON ai_cache(created_at DESC)
  `).catch(() => null);
}

export function buildAiCacheKey(input: {
  clientId: string;
  question: string;
  history?: any[];
  provider?: string;
  routeHint?: string;
  context?: any;
}) {
  const hash = crypto
    .createHash("sha256")
    .update(
      stableStringify({
        clientId: input.clientId,
        question: input.question,
        history: input.history || [],
        provider: input.provider || "anthropic",
        routeHint: input.routeHint || "",
        context: input.context || {},
      }),
    )
    .digest("hex");

  return `ai:${input.clientId}:${hash}`;
}

export type AiRoutePlan = {
  provider: "anthropic" | "fallback";
  route: string;
  reason: string;
  model: string | null;
};

export function chooseAiRoute(args: {
  provider: string;
  question: string;
  history: Array<{ role: "user" | "assistant"; text: string }>;
  context: any;
  anthropicModel?: string | null;
}): AiRoutePlan {
  const provider = String(args.provider || "anthropic")
    .trim()
    .toLowerCase();
  const q = String(args.question || "").toLowerCase();
  const asksSop = /sop|process|procedure|sharepoint|document|reference/.test(q);
  const asksComms =
    /communication|timeline|follow-up|follow up|email|call|meeting|conversation/.test(
      q,
    );
  const asksStrategy =
    /next best|what should|recommend|opportunit|grow|upsell|risk|churn/.test(q);
  const longHistory = Array.isArray(args.history) && args.history.length >= 5;
  const sopCount = Number(args?.context?.sops?.count ?? 0);
  const commCount = Number(args?.context?.communications?.count ?? 0);

  if (provider !== "anthropic") {
    return {
      provider: "fallback",
      route: "fallback-rule-engine",
      reason: "AI_PROVIDER is not anthropic.",
      model: null,
    };
  }

  if (asksComms || commCount > 0) {
    return {
      provider: "anthropic",
      route: "anthropic-communications-aware",
      reason: "Communication timeline reasoning requested or available.",
      model: args.anthropicModel || "claude-sonnet-4-6",
    };
  }

  if (sopCount > 0 || asksSop) {
    return {
      provider: "anthropic",
      route: "anthropic-sop-aware",
      reason: "SOP-heavy or SharePoint-aware request.",
      model: args.anthropicModel || "claude-sonnet-4-6",
    };
  }

  if (asksStrategy || longHistory) {
    return {
      provider: "anthropic",
      route: "anthropic-strategy",
      reason: "Strategic reasoning, risk, or next-best-action request.",
      model: args.anthropicModel || "claude-sonnet-4-6",
    };
  }

  return {
    provider: "anthropic",
    route: "anthropic-default",
    reason: "Default Anthropic route for client intelligence.",
    model: args.anthropicModel || "claude-sonnet-4-6",
  };
}

export async function getCachedAiPayload<T = CacheablePayload>(
  cacheKey: string,
  maxAgeHours = 24,
): Promise<T | null> {
  const r = await query<{ payload: T }>(
    `
    SELECT payload
    FROM ai_cache
    WHERE cache_key = $1
      AND updated_at >= NOW() - ($2::text || ' hours')::interval
    LIMIT 1
    `,
    [cacheKey, String(maxAgeHours)],
  ).catch(() => ({ rows: [] }) as any);

  return r.rows?.[0]?.payload || null;
}

export async function setCachedAiPayload(
  cacheKey: string,
  payload: CacheablePayload,
) {
  await query(
    `
    INSERT INTO ai_cache (cache_key, payload, created_at, updated_at)
    VALUES ($1, $2::jsonb, NOW(), NOW())
    ON CONFLICT (cache_key)
    DO UPDATE SET
      payload = EXCLUDED.payload,
      updated_at = NOW()
    `,
    [cacheKey, JSON.stringify(payload)],
  ).catch(() => null);
}

export async function logAiInteraction(args: {
  clientId?: string | null;
  userId?: string | null;
  prompt: string;
  response?: string | null;
  provider?: string | null;
  model?: string | null;
  cacheKey?: string | null;
  cached?: boolean;
  routeReason?: string | null;
  requestId?: string | null;
  meta?: Record<string, any> | null;
}) {
  await query(
    `
    INSERT INTO ai_logs (
      client_id,
      user_id,
      prompt,
      response,
      provider,
      model,
      cache_key,
      cached,
      route_reason,
      request_id,
      meta
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb
    )
    `,
    [
      args.clientId || null,
      args.userId || null,
      args.prompt,
      args.response || null,
      args.provider || null,
      args.model || null,
      args.cacheKey || null,
      Boolean(args.cached),
      args.routeReason || null,
      args.requestId || null,
      JSON.stringify(args.meta || {}),
    ],
  ).catch(() => null);
}

export async function fetchRecentAiLogs(limit = 50) {
  const r = await query(
    `
    SELECT
      id,
      client_id,
      user_id,
      prompt,
      response,
      provider,
      model,
      cache_key,
      cached,
      route_reason,
      request_id,
      meta,
      created_at
    FROM ai_logs
    ORDER BY created_at DESC
    LIMIT $1
    `,
    [limit],
  ).catch(() => ({ rows: [] }) as any);

  return r.rows || [];
}
