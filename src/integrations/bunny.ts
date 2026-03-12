// services/api/src/integrations/bunny.ts
import path from "node:path";

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function stripSlashes(value: string) {
  return String(value || "").replace(/^\/+|\/+$/g, "");
}

export function hasBunnyStorage() {
  return Boolean(
    env("BUNNY_STORAGE_ZONE") &&
    env("BUNNY_STORAGE_PASSWORD") &&
    env("BUNNY_STORAGE_HOSTNAME") &&
    env("BUNNY_PULL_ZONE_URL"),
  );
}

function sanitizeSegment(value: string) {
  return (
    String(value || "")
      .trim()
      .replace(/[^a-zA-Z0-9._-]+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "") || "file"
  );
}

function encodeStoragePath(storagePath: string) {
  return stripSlashes(storagePath)
    .split("/")
    .filter(Boolean)
    .map(encodeURIComponent)
    .join("/");
}

export function buildTaskAttachmentStoragePath(args: {
  clientId: string;
  taskId: string;
  fileName: string;
}) {
  const ext = path.extname(args.fileName || "");
  const base = path.basename(args.fileName || "file", ext);
  const stamped = `${Date.now()}-${sanitizeSegment(base)}${ext}`;
  return [
    "tasks",
    sanitizeSegment(args.clientId),
    sanitizeSegment(args.taskId),
    stamped,
  ].join("/");
}

export async function uploadBufferToBunny(args: {
  storagePath: string;
  buffer: Buffer;
  contentType?: string;
}) {
  const zone = env("BUNNY_STORAGE_ZONE");
  const password = env("BUNNY_STORAGE_PASSWORD");
  const hostname = env(
    "BUNNY_STORAGE_HOSTNAME",
    env("BUNNY_STORAGE_REGION")
      ? `${env("BUNNY_STORAGE_REGION")}.storage.bunnycdn.com`
      : "",
  );
  const publicBaseUrl = env("BUNNY_PULL_ZONE_URL");

  if (!zone || !password || !hostname || !publicBaseUrl) {
    throw new Error("Bunny storage is not configured correctly.");
  }

  const normalizedPath = stripSlashes(args.storagePath);
  const encodedPath = encodeStoragePath(normalizedPath);
  const uploadUrl = `https://${stripSlashes(hostname)}/${encodeURIComponent(zone)}/${encodedPath}`;

  const res = await fetch(uploadUrl, {
    method: "PUT",
    headers: {
      AccessKey: password,
      "Content-Type": args.contentType || "application/octet-stream",
    },
    body: args.buffer,
  });

  if (!res.ok) {
    const raw = await res.text().catch(() => "");
    throw new Error(raw || `Bunny upload failed (${res.status})`);
  }

  return {
    storagePath: normalizedPath,
    fileUrl: `${publicBaseUrl.replace(/\/+$/, "")}/${normalizedPath}`,
  };
}

export async function deleteFromBunny(storagePath: string) {
  const zone = env("BUNNY_STORAGE_ZONE");
  const password = env("BUNNY_STORAGE_PASSWORD");
  const hostname = env(
    "BUNNY_STORAGE_HOSTNAME",
    env("BUNNY_STORAGE_REGION")
      ? `${env("BUNNY_STORAGE_REGION")}.storage.bunnycdn.com`
      : "",
  );

  if (!zone || !password || !hostname) {
    throw new Error("Bunny storage is not configured correctly.");
  }

  const normalizedPath = stripSlashes(storagePath);
  const encodedPath = encodeStoragePath(normalizedPath);
  const deleteUrl = `https://${stripSlashes(hostname)}/${encodeURIComponent(zone)}/${encodedPath}`;

  const res = await fetch(deleteUrl, {
    method: "DELETE",
    headers: {
      AccessKey: password,
    },
  });

  if (!res.ok) {
    const raw = await res.text().catch(() => "");
    throw new Error(raw || `Bunny delete failed (${res.status})`);
  }

  return { ok: true };
}
