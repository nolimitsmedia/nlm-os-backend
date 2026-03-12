import { Router } from "express";
import multer from "multer";
import { requireAuth } from "../middleware/auth.js";
import {
  createFolder,
  getSharePointStatus,
  listDrives,
  listItems,
  listSites,
  uploadFile,
} from "../integrations/sharepoint.js";

const router = Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 25 * 1024 * 1024,
    files: 10,
  },
});

router.get("/status", requireAuth, async (_req, res) => {
  try {
    return res.json(getSharePointStatus());
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to get SharePoint status",
    });
  }
});

router.get("/sites", requireAuth, async (req, res) => {
  try {
    const search = String(req.query.search || "").trim();
    const items = await listSites(search);
    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to load SharePoint sites",
    });
  }
});

router.get("/drives", requireAuth, async (req, res) => {
  try {
    const siteId = String(req.query.siteId || "").trim();
    const items = await listDrives(siteId);
    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to load SharePoint drives",
    });
  }
});

router.get("/items", requireAuth, async (req, res) => {
  try {
    const siteId = String(req.query.siteId || "").trim();
    const driveId = String(req.query.driveId || "").trim();
    const path = String(req.query.path || "").trim();

    const items = await listItems({
      siteId,
      driveId,
      path,
    });

    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to load SharePoint items",
    });
  }
});

router.post("/folders", requireAuth, async (req, res) => {
  try {
    const siteId = String(req.body?.siteId || "").trim();
    const driveId = String(req.body?.driveId || "").trim();
    const parentPath = String(req.body?.parentPath || "").trim();
    const name = String(req.body?.name || "").trim();

    const result = await createFolder({
      siteId,
      driveId,
      parentPath,
      name,
    });

    return res.json(result);
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to create SharePoint folder",
    });
  }
});

router.post("/upload", requireAuth, upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    const siteId = String(req.body?.siteId || "").trim();
    const driveId = String(req.body?.driveId || "").trim();
    const folderPath = String(req.body?.folderPath || "").trim();

    if (!file) {
      return res.status(400).json({
        ok: false,
        error: "No file uploaded",
      });
    }

    const result = await uploadFile({
      siteId,
      driveId,
      folderPath,
      fileName: file.originalname,
      buffer: file.buffer,
      contentType: file.mimetype,
    });

    return res.json(result);
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to upload SharePoint file",
    });
  }
});

export default router;
