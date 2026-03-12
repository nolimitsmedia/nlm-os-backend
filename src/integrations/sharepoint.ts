// services/api/src/integrations/sharepoint.ts

import { getGraphConfig, graphFetch, isGraphMockMode } from "./graph.js";

export type SharePointSite = {
  id: string;
  name: string;
  webUrl?: string | null;
};

export type SharePointDrive = {
  id: string;
  name: string;
  webUrl?: string | null;
};

export type SharePointItem = {
  id: string;
  name: string;
  type: "file" | "folder";
  path: string;
  webUrl?: string | null;
  size?: number | null;
  lastModified?: string | null;
  mimeType?: string | null;
};

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function normalizePath(path?: string | null) {
  const raw = String(path || "").trim();
  if (!raw || raw === "/") return "";
  return raw.replace(/^\/+/, "").replace(/\/+$/, "");
}

function joinPath(base?: string | null, name?: string | null) {
  const a = normalizePath(base);
  const b = String(name || "")
    .trim()
    .replace(/^\/+/, "")
    .replace(/\/+$/, "");

  if (!a) return b;
  if (!b) return a;
  return `${a}/${b}`;
}

function encodeGraphPath(path?: string | null) {
  const clean = normalizePath(path);
  return clean.split("/").filter(Boolean).map(encodeURIComponent).join("/");
}

function mapGraphItem(item: any): SharePointItem {
  const parentPath = String(item?.parentReference?.path || "");
  const cleanedParent = parentPath
    .replace(/^.*\/root:/i, "")
    .replace(/^\/+/, "");
  const fullPath = joinPath(cleanedParent, item?.name);

  return {
    id: String(item?.id || ""),
    name: String(item?.name || ""),
    type: item?.folder ? "folder" : "file",
    path: `/${fullPath}`,
    webUrl: item?.webUrl || null,
    size: Number.isFinite(Number(item?.size)) ? Number(item.size) : null,
    lastModified: item?.lastModifiedDateTime || null,
    mimeType: item?.file?.mimeType || null,
  };
}

export function getSharePointStatus() {
  const cfg = getGraphConfig();

  return {
    ok: true,
    configured: cfg.configured,
    mock: cfg.mock,
    hasTenantId: Boolean(cfg.tenantId),
    hasClientId: Boolean(cfg.clientId),
    hasClientSecret: Boolean(cfg.clientSecret),
    defaultSiteId: env("MS_SHAREPOINT_SITE_ID"),
    defaultDriveId: env("MS_SHAREPOINT_DRIVE_ID"),
  };
}

export function isSharePointConfigured() {
  const status = getSharePointStatus();
  return Boolean(status.configured);
}

export async function listSites(search?: string): Promise<SharePointSite[]> {
  if (isGraphMockMode()) {
    return [
      {
        id: "mock-site-1",
        name: "No Limits Media SharePoint",
        webUrl: "https://example.sharepoint.com/sites/nlm",
      },
      {
        id: "mock-site-2",
        name: "Client Documents",
        webUrl: "https://example.sharepoint.com/sites/clients",
      },
    ].filter((x) =>
      search ? x.name.toLowerCase().includes(search.toLowerCase()) : true,
    );
  }

  const q = String(search || "").trim();
  const path = q ? `/sites?search=${encodeURIComponent(q)}` : `/sites?search=*`;

  const data: any = await graphFetch(path);
  const items = Array.isArray(data?.value) ? data.value : [];

  return items.map((site: any) => ({
    id: String(site?.id || ""),
    name: String(site?.displayName || site?.name || ""),
    webUrl: site?.webUrl || null,
  }));
}

export async function listDrives(siteId?: string): Promise<SharePointDrive[]> {
  const fallbackSiteId = env("MS_SHAREPOINT_SITE_ID");
  const resolvedSiteId = String(siteId || fallbackSiteId || "").trim();

  if (!resolvedSiteId) {
    if (isGraphMockMode()) {
      return [
        { id: "mock-drive-1", name: "Documents", webUrl: null },
        { id: "mock-drive-2", name: "Shared", webUrl: null },
      ];
    }
    throw new Error("Missing SharePoint site id.");
  }

  if (isGraphMockMode()) {
    return [
      { id: "mock-drive-1", name: "Documents", webUrl: null },
      { id: "mock-drive-2", name: "Shared", webUrl: null },
    ];
  }

  const data: any = await graphFetch(
    `/sites/${encodeURIComponent(resolvedSiteId)}/drives`,
  );
  const items = Array.isArray(data?.value) ? data.value : [];

  return items.map((drive: any) => ({
    id: String(drive?.id || ""),
    name: String(drive?.name || ""),
    webUrl: drive?.webUrl || null,
  }));
}

export async function listItems(params?: {
  siteId?: string;
  driveId?: string;
  path?: string;
}): Promise<SharePointItem[]> {
  const siteId = String(
    params?.siteId || env("MS_SHAREPOINT_SITE_ID") || "",
  ).trim();
  const driveId = String(
    params?.driveId || env("MS_SHAREPOINT_DRIVE_ID") || "",
  ).trim();
  const path = normalizePath(params?.path);

  if (isGraphMockMode()) {
    if (!path) {
      return [
        {
          id: "folder-1",
          name: "Clients",
          type: "folder",
          path: "/Clients",
          webUrl: null,
          size: null,
          lastModified: "2026-03-11T08:00:00.000Z",
          mimeType: null,
        },
        {
          id: "folder-2",
          name: "Internal Docs",
          type: "folder",
          path: "/Internal Docs",
          webUrl: null,
          size: null,
          lastModified: "2026-03-10T15:30:00.000Z",
          mimeType: null,
        },
        {
          id: "file-1",
          name: "NLM-Overview.pdf",
          type: "file",
          path: "/NLM-Overview.pdf",
          webUrl: null,
          size: 248320,
          lastModified: "2026-03-09T10:15:00.000Z",
          mimeType: "application/pdf",
        },
      ];
    }

    if (path === "Clients") {
      return [
        {
          id: "folder-3",
          name: "Bishop Robertson",
          type: "folder",
          path: "/Clients/Bishop Robertson",
          webUrl: null,
          size: null,
          lastModified: "2026-03-11T09:12:00.000Z",
          mimeType: null,
        },
        {
          id: "folder-4",
          name: "Mt Gilead",
          type: "folder",
          path: "/Clients/Mt Gilead",
          webUrl: null,
          size: null,
          lastModified: "2026-03-08T11:05:00.000Z",
          mimeType: null,
        },
      ];
    }

    return [
      {
        id: "file-2",
        name: "Project-Plan.docx",
        type: "file",
        path: `/${joinPath(path, "Project-Plan.docx")}`,
        webUrl: null,
        size: 58312,
        lastModified: "2026-03-11T09:20:00.000Z",
        mimeType:
          "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      },
      {
        id: "file-3",
        name: "Asset-List.xlsx",
        type: "file",
        path: `/${joinPath(path, "Asset-List.xlsx")}`,
        webUrl: null,
        size: 112004,
        lastModified: "2026-03-10T13:45:00.000Z",
        mimeType:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      },
    ];
  }

  if (!siteId) throw new Error("Missing SharePoint site id.");
  if (!driveId) throw new Error("Missing SharePoint drive id.");

  const endpoint = path
    ? `/sites/${encodeURIComponent(siteId)}/drives/${encodeURIComponent(
        driveId,
      )}/root:/${encodeGraphPath(path)}:/children`
    : `/sites/${encodeURIComponent(siteId)}/drives/${encodeURIComponent(
        driveId,
      )}/root/children`;

  const data: any = await graphFetch(endpoint);
  const items = Array.isArray(data?.value) ? data.value : [];
  return items.map(mapGraphItem);
}

export async function createFolder(params: {
  siteId?: string;
  driveId?: string;
  parentPath?: string;
  name: string;
}) {
  const siteId = String(
    params?.siteId || env("MS_SHAREPOINT_SITE_ID") || "",
  ).trim();
  const driveId = String(
    params?.driveId || env("MS_SHAREPOINT_DRIVE_ID") || "",
  ).trim();
  const parentPath = normalizePath(params?.parentPath);
  const name = String(params?.name || "").trim();

  if (!name) {
    throw new Error("Folder name is required.");
  }

  if (isGraphMockMode()) {
    return {
      ok: true,
      item: {
        id: `mock-folder-${Date.now()}`,
        name,
        type: "folder" as const,
        path: `/${joinPath(parentPath, name)}`,
        webUrl: null,
        size: null,
        lastModified: new Date().toISOString(),
        mimeType: null,
      },
    };
  }

  if (!siteId) throw new Error("Missing SharePoint site id.");
  if (!driveId) throw new Error("Missing SharePoint drive id.");

  const endpoint = parentPath
    ? `/sites/${encodeURIComponent(siteId)}/drives/${encodeURIComponent(
        driveId,
      )}/root:/${encodeGraphPath(parentPath)}:/children`
    : `/sites/${encodeURIComponent(siteId)}/drives/${encodeURIComponent(
        driveId,
      )}/root/children`;

  const data: any = await graphFetch(endpoint, {
    method: "POST",
    body: JSON.stringify({
      name,
      folder: {},
      "@microsoft.graph.conflictBehavior": "rename",
    }),
  });

  return {
    ok: true,
    item: mapGraphItem(data),
  };
}

export async function uploadFile(params: {
  siteId?: string;
  driveId?: string;
  folderPath?: string;
  fileName: string;
  buffer: Buffer;
  contentType?: string;
}) {
  const siteId = String(
    params?.siteId || env("MS_SHAREPOINT_SITE_ID") || "",
  ).trim();
  const driveId = String(
    params?.driveId || env("MS_SHAREPOINT_DRIVE_ID") || "",
  ).trim();
  const folderPath = normalizePath(params?.folderPath);
  const fileName = String(params?.fileName || "").trim();

  if (!fileName) {
    throw new Error("File name is required.");
  }

  if (!params?.buffer || !Buffer.isBuffer(params.buffer)) {
    throw new Error("Upload buffer is required.");
  }

  if (isGraphMockMode()) {
    return {
      ok: true,
      item: {
        id: `mock-file-${Date.now()}`,
        name: fileName,
        type: "file" as const,
        path: `/${joinPath(folderPath, fileName)}`,
        webUrl: null,
        size: params.buffer.length,
        lastModified: new Date().toISOString(),
        mimeType: params.contentType || null,
      },
    };
  }

  if (!siteId) throw new Error("Missing SharePoint site id.");
  if (!driveId) throw new Error("Missing SharePoint drive id.");

  const fullPath = joinPath(folderPath, fileName);
  const endpoint = `/sites/${encodeURIComponent(siteId)}/drives/${encodeURIComponent(
    driveId,
  )}/root:/${encodeGraphPath(fullPath)}:/content`;

  const data: any = await graphFetch(endpoint, {
    method: "PUT",
    headers: {
      "Content-Type": params.contentType || "application/octet-stream",
    },
    body: params.buffer,
  });

  return {
    ok: true,
    item: mapGraphItem(data),
  };
}
