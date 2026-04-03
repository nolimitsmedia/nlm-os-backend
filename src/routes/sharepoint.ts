// services/api/src/routes/sharepoint.ts
import { Router } from "express";
import multer from "multer";
import { query } from "../db.js";
import { requireAuth, requireRole } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";
import {
  createFolder,
  getSharePointStatus,
  listDrives,
  listItems,
  listSites,
  uploadFile,
} from "../integrations/sharepoint.js";

const router = Router();
const SHAREPOINT_WRITE_ROLES = ["admin", "operations", "tech"];
const FOLDER_TEMPLATES = [
  "onboarding",
  "billing",
  "SOPs",
  "project docs",
] as const;

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024, files: 10 },
});

async function ensureSharePointLinksTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS sharepoint_links (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id TEXT,
      task_id TEXT,
      sop_id TEXT,
      site_id TEXT,
      drive_id TEXT,
      item_id TEXT,
      item_name TEXT,
      item_type TEXT,
      item_path TEXT,
      web_url TEXT,
      mime_type TEXT,
      source TEXT DEFAULT 'sharepoint',
      tags JSONB DEFAULT '[]'::jsonb,
      meta JSONB DEFAULT '{}'::jsonb,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `).catch(() => null);

  await query(
    `CREATE INDEX IF NOT EXISTS idx_sharepoint_links_client_id ON sharepoint_links(client_id)`,
  ).catch(() => null);
  await query(
    `CREATE INDEX IF NOT EXISTS idx_sharepoint_links_task_id ON sharepoint_links(task_id)`,
  ).catch(() => null);
  await query(
    `CREATE INDEX IF NOT EXISTS idx_sharepoint_links_sop_id ON sharepoint_links(sop_id)`,
  ).catch(() => null);
}

function clean(value: any) {
  return String(value || "").trim();
}

function parseTags(value: any): string[] {
  if (Array.isArray(value))
    return value.map((x) => String(x || "").trim()).filter(Boolean);
  const raw = clean(value);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed))
      return parsed.map((x) => String(x || "").trim()).filter(Boolean);
  } catch {}
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseMeta(value: any): Record<string, any> {
  if (value && typeof value === "object" && !Array.isArray(value))
    return value as Record<string, any>;
  const raw = clean(value);
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed
      : {};
  } catch {
    return {};
  }
}

function makeClientFolderName(clientId: string) {
  return (
    clean(clientId)
      .replace(/[^a-zA-Z0-9-_ ]/g, " ")
      .replace(/\s+/g, " ")
      .trim() || "Client"
  );
}

async function safeCreateFolder(
  siteId: string,
  driveId: string,
  parentPath: string,
  name: string,
) {
  return createFolder({ siteId, driveId, parentPath, name }).catch(
    async (e: any) => {
      const msg = String(e?.message || "");
      if (/already exists/i.test(msg) || /nameAlreadyExists/i.test(msg)) {
        return {
          ok: true,
          name,
          path: [parentPath, name].filter(Boolean).join("/"),
        } as any;
      }
      throw e;
    },
  );
}

router.get("/status", requireAuth, async (_req, res) => {
  try {
    const status = getSharePointStatus();
    const fallbackMode = !status.configured || Boolean(status.mock);
    return res.json({
      ...status,
      fallback_mode: fallbackMode,
      fallback_reason: fallbackMode
        ? "Live Microsoft Graph access is not fully available yet. Manual and hybrid SharePoint workflows remain active."
        : null,
      recommendations: [
        !status.configured
          ? "Add Microsoft Graph tenant, client ID, and client secret."
          : null,
        !status.defaultSiteId
          ? "Set a default SharePoint site ID for faster browsing."
          : null,
        !status.defaultDriveId
          ? "Set a default SharePoint drive ID to preselect the document library."
          : null,
      ].filter(Boolean),
    });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to get SharePoint status",
      });
  }
});

router.get("/sites", requireAuth, async (req, res) => {
  try {
    const search = clean(req.query.search);
    const items = await listSites(search);
    return res.json({ ok: true, items });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to load SharePoint sites",
      });
  }
});

router.get("/drives", requireAuth, async (req, res) => {
  try {
    const siteId = clean(req.query.siteId);
    const items = await listDrives(siteId);
    return res.json({ ok: true, items });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to load SharePoint drives",
      });
  }
});

router.get("/items", requireAuth, async (req, res) => {
  try {
    const siteId = clean(req.query.siteId);
    const driveId = clean(req.query.driveId);
    const path = clean(req.query.path);
    const items = await listItems({ siteId, driveId, path });
    return res.json({ ok: true, items });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to load SharePoint items",
      });
  }
});

router.post(
  "/folders",
  requireAuth,
  requireRole(SHAREPOINT_WRITE_ROLES),
  async (req: any, res) => {
    try {
      const siteId = clean(req.body?.siteId);
      const driveId = clean(req.body?.driveId);
      const parentPath = clean(req.body?.parentPath);
      const name = clean(req.body?.name);

      const result = await createFolder({ siteId, driveId, parentPath, name });

      await writeAudit({
        user_id: req.user?.id ? String(req.user.id) : null,
        action: "create",
        entity: "sharepoint_folder",
        entity_id: clean(result?.id || result?.itemId) || null,
        client_id: clean(req.body?.clientId) || null,
        meta: { siteId, driveId, parentPath, name },
        ip: req.ip,
      });

      return res.json({ ok: true, folder: result });
    } catch (e: any) {
      return res
        .status(500)
        .json({
          ok: false,
          error: e?.message || "Failed to create SharePoint folder",
        });
    }
  },
);

router.post(
  "/folders/templates",
  requireAuth,
  requireRole(SHAREPOINT_WRITE_ROLES),
  async (req: any, res) => {
    try {
      const siteId = clean(req.body?.siteId);
      const driveId = clean(req.body?.driveId);
      const parentPath = clean(req.body?.parentPath);
      const clientId = clean(req.body?.clientId);
      const clientFolder = makeClientFolderName(
        clean(req.body?.clientName || clientId),
      );
      const basePath = [parentPath, clientFolder].filter(Boolean).join("/");

      const created: Array<{ name: string; path: string }> = [];
      await safeCreateFolder(siteId, driveId, parentPath, clientFolder);
      for (const name of FOLDER_TEMPLATES) {
        await safeCreateFolder(siteId, driveId, basePath, name);
        created.push({
          name,
          path: [basePath, name].filter(Boolean).join("/"),
        });
      }

      await writeAudit({
        user_id: req.user?.id ? String(req.user.id) : null,
        action: "create",
        entity: "sharepoint_folder_template",
        entity_id: null,
        client_id: clientId || null,
        meta: {
          siteId,
          driveId,
          parentPath,
          basePath,
          templates: FOLDER_TEMPLATES,
        },
        ip: req.ip,
      });

      return res.json({
        ok: true,
        client_folder: basePath,
        templates: created,
      });
    } catch (e: any) {
      return res
        .status(500)
        .json({
          ok: false,
          error: e?.message || "Failed to create folder templates",
        });
    }
  },
);

router.post(
  "/upload",
  requireAuth,
  requireRole(SHAREPOINT_WRITE_ROLES),
  upload.single("file"),
  async (req: any, res) => {
    try {
      const file = req.file;
      const siteId = clean(req.body?.siteId);
      const driveId = clean(req.body?.driveId);
      const folderPath = clean(req.body?.folderPath);
      const clientId = clean(req.body?.clientId) || null;
      const taskId = clean(req.body?.taskId) || null;
      const sopId = clean(req.body?.sopId) || null;
      const tags = parseTags(req.body?.tags);
      const meta = parseMeta(req.body?.meta);

      if (!file)
        return res.status(400).json({ ok: false, error: "No file uploaded" });

      const result = await uploadFile({
        siteId,
        driveId,
        folderPath,
        fileName: file.originalname,
        buffer: file.buffer,
        contentType: file.mimetype,
      });

      await ensureSharePointLinksTable();
      const inserted = await query(
        `INSERT INTO sharepoint_links (
        client_id, task_id, sop_id, site_id, drive_id, item_id, item_name, item_type, item_path, web_url, mime_type, tags, meta, created_by
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14)
      RETURNING *`,
        [
          clientId,
          taskId,
          sopId,
          siteId,
          driveId,
          clean(result?.id || result?.itemId) || null,
          clean(result?.name || file.originalname),
          "file",
          clean(
            result?.path ||
              [folderPath, file.originalname].filter(Boolean).join("/"),
          ),
          clean(result?.webUrl || result?.url) || null,
          clean(file.mimetype) || null,
          JSON.stringify(tags),
          JSON.stringify({
            ...meta,
            uploaded_via: "sharepoint_upload",
            size: Number(file.size || 0),
          }),
          req.user?.id ? String(req.user.id) : null,
        ],
      ).catch(() => ({ rows: [] }) as any);

      await writeAudit({
        user_id: req.user?.id ? String(req.user.id) : null,
        action: "upload",
        entity: "sharepoint_file",
        entity_id: clean(result?.id || result?.itemId) || null,
        client_id: clientId,
        meta: {
          siteId,
          driveId,
          folderPath,
          taskId,
          sopId,
          fileName: file.originalname,
          tags,
          meta,
        },
        ip: req.ip,
      });

      return res.json({
        ok: true,
        item: result,
        link: inserted.rows?.[0] || null,
      });
    } catch (e: any) {
      return res
        .status(500)
        .json({
          ok: false,
          error: e?.message || "Failed to upload SharePoint file",
        });
    }
  },
);

router.post(
  "/links",
  requireAuth,
  requireRole(SHAREPOINT_WRITE_ROLES),
  async (req: any, res) => {
    try {
      await ensureSharePointLinksTable();
      const clientId = clean(req.body?.clientId) || null;
      const taskId = clean(req.body?.taskId) || null;
      const sopId = clean(req.body?.sopId) || null;
      const siteId = clean(req.body?.siteId) || null;
      const driveId = clean(req.body?.driveId) || null;
      const itemId = clean(req.body?.itemId) || null;
      const itemName =
        clean(req.body?.itemName || req.body?.name) || "SharePoint item";
      const itemType = clean(req.body?.itemType || "file") || "file";
      const itemPath = clean(req.body?.itemPath || req.body?.path) || null;
      const webUrl = clean(req.body?.webUrl) || null;
      const mimeType = clean(req.body?.mimeType) || null;
      const tags = parseTags(req.body?.tags);
      const meta = parseMeta(req.body?.meta);

      const r = await query(
        `INSERT INTO sharepoint_links (
        client_id, task_id, sop_id, site_id, drive_id, item_id, item_name, item_type, item_path, web_url, mime_type, tags, meta, created_by
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14)
      RETURNING *`,
        [
          clientId,
          taskId,
          sopId,
          siteId,
          driveId,
          itemId,
          itemName,
          itemType,
          itemPath,
          webUrl,
          mimeType,
          JSON.stringify(tags),
          JSON.stringify(meta),
          req.user?.id ? String(req.user.id) : null,
        ],
      );

      await writeAudit({
        user_id: req.user?.id ? String(req.user.id) : null,
        action: "attach",
        entity: "sharepoint_link",
        entity_id: String(r.rows?.[0]?.id || "") || null,
        client_id: clientId,
        meta: {
          taskId,
          sopId,
          itemId,
          itemName,
          itemType,
          itemPath,
          webUrl,
          tags,
          meta,
        },
        ip: req.ip,
      });

      return res.status(201).json({ ok: true, link: r.rows?.[0] || null });
    } catch (e: any) {
      return res
        .status(500)
        .json({
          ok: false,
          error: e?.message || "Failed to attach SharePoint item",
        });
    }
  },
);

router.get("/links", requireAuth, async (req, res) => {
  try {
    await ensureSharePointLinksTable();
    const clientId = clean(req.query.clientId);
    const taskId = clean(req.query.taskId);
    const sopId = clean(req.query.sopId);

    const clauses = [] as string[];
    const params = [] as any[];
    if (clientId) {
      params.push(clientId);
      clauses.push(`client_id = $${params.length}`);
    }
    if (taskId) {
      params.push(taskId);
      clauses.push(`task_id = $${params.length}`);
    }
    if (sopId) {
      params.push(sopId);
      clauses.push(`sop_id = $${params.length}`);
    }

    const sql = `SELECT * FROM sharepoint_links ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""} ORDER BY created_at DESC LIMIT 100`;
    const r = await query(sql, params);
    return res.json({ ok: true, items: r.rows || [] });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to load SharePoint links",
      });
  }
});

router.get("/client-shortcuts", requireAuth, async (req, res) => {
  try {
    const clientId = clean(req.query.clientId);
    const siteId = clean(req.query.siteId);
    const driveId = clean(req.query.driveId);
    const clientName = makeClientFolderName(
      clean(req.query.clientName || clientId),
    );
    const items = await listItems({
      siteId,
      driveId,
      path: clientName ? `/${clientName}` : "/",
    }).catch(() => []);
    const shortcuts = FOLDER_TEMPLATES.map((name) => ({
      key: name,
      label: name,
      path: [clientName, name].filter(Boolean).join("/"),
    }));
    return res.json({
      ok: true,
      clientId,
      clientFolder: clientName,
      shortcuts,
      items: Array.isArray(items) ? items : [],
    });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to load client shortcuts",
      });
  }
});

router.get("/workspace-summary", requireAuth, async (req, res) => {
  try {
    const siteId = clean(req.query.siteId);
    const driveId = clean(req.query.driveId);
    const status = getSharePointStatus();
    const sites = await listSites("").catch(() => []);
    const drives = await listDrives(siteId).catch(() => []);
    const rootItems = driveId
      ? await listItems({ siteId, driveId, path: "/" }).catch(() => [])
      : [];

    return res.json({
      ok: true,
      status: {
        ...status,
        fallback_mode: !status.configured || Boolean(status.mock),
        fallback_reason:
          !status.configured || Boolean(status.mock)
            ? "Hybrid fallback mode is active while Graph is not fully live."
            : null,
      },
      summary: {
        site_count: Array.isArray(sites) ? sites.length : 0,
        drive_count: Array.isArray(drives) ? drives.length : 0,
        root_item_count: Array.isArray(rootItems) ? rootItems.length : 0,
      },
    });
  } catch (e: any) {
    return res
      .status(500)
      .json({
        ok: false,
        error: e?.message || "Failed to load SharePoint workspace summary",
      });
  }
});

export default router;
