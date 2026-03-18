// services/api/src/integrations/graph.ts

type GraphTokenCache = {
  accessToken: string;
  expiresAt: number;
} | null;

let tokenCache: GraphTokenCache = null;

export function clearGraphTokenCache() {
  tokenCache = null;
}

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

export function isGraphMockMode() {
  const raw = env("MS_GRAPH_MOCK", "true").toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
}

export function getGraphConfig() {
  const tenantId = env("MS_TENANT_ID");
  const clientId = env("MS_CLIENT_ID");
  const clientSecret = env("MS_CLIENT_SECRET");

  return {
    tenantId,
    clientId,
    clientSecret,
    configured: Boolean(tenantId && clientId && clientSecret),
    mock: isGraphMockMode(),
  };
}

export async function getGraphAccessToken() {
  const { tenantId, clientId, clientSecret, configured, mock } =
    getGraphConfig();

  if (mock) {
    throw new Error("Microsoft Graph is running in mock mode.");
  }

  if (!configured) {
    throw new Error("Microsoft Graph credentials are not configured.");
  }

  const now = Date.now();
  if (tokenCache && tokenCache.expiresAt > now + 60_000) {
    return tokenCache.accessToken;
  }

  const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;

  const body = new URLSearchParams();
  body.set("client_id", clientId);
  body.set("client_secret", clientSecret);
  body.set("scope", "https://graph.microsoft.com/.default");
  body.set("grant_type", "client_credentials");

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
  });

  const data: any = await res.json().catch(() => ({}));

  if (!res.ok) {
    clearGraphTokenCache();
    throw new Error(
      data?.error_description ||
        data?.error?.message ||
        `Graph token request failed (${res.status})`,
    );
  }

  const accessToken = String(data?.access_token || "");
  const expiresIn = Number(data?.expires_in || 3600);

  if (!accessToken) {
    throw new Error("Graph token response did not include an access token.");
  }

  tokenCache = {
    accessToken,
    expiresAt: now + expiresIn * 1000,
  };

  return accessToken;
}

export async function graphFetch(path: string, init: RequestInit = {}) {
  const token = await getGraphAccessToken();

  const url = path.startsWith("http")
    ? path
    : `https://graph.microsoft.com/v1.0${path.startsWith("/") ? path : `/${path}`}`;

  const headers = new Headers(init.headers || {});
  headers.set("Authorization", `Bearer ${token}`);

  if (
    !headers.has("Content-Type") &&
    init.body &&
    !(init.body instanceof FormData)
  ) {
    headers.set("Content-Type", "application/json");
  }

  const res = await fetch(url, {
    ...init,
    headers,
  });

  const contentType = res.headers.get("content-type") || "";
  let payload: any = null;

  if (contentType.includes("application/json")) {
    payload = await res.json().catch(() => null);
  } else {
    payload = await res.text().catch(() => "");
  }

  if (!res.ok) {
    const message =
      payload?.error?.message ||
      payload?.message ||
      (typeof payload === "string" ? payload : "") ||
      `Graph request failed (${res.status})`;

    if (res.status === 401 || res.status === 403) clearGraphTokenCache();
    throw new Error(message);
  }

  return payload;
}
