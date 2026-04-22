// services/api/src/routes/notifications.ts
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";

const router = Router();

async function ensureNotificationsTable() {
  try {
    await query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`).catch(() => null);
    await query(`
      CREATE TABLE IF NOT EXISTS public.notifications (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id uuid NOT NULL,
        kind text NOT NULL DEFAULT 'info',
        title text NOT NULL,
        body text,
        action_url text,
        action_label text,
        meta jsonb NOT NULL DEFAULT '{}'::jsonb,
        read_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now()
      )
    `).catch(() => null);

    await query(`
      CREATE INDEX IF NOT EXISTS idx_notifications_user_created
      ON public.notifications (user_id, created_at DESC)
    `).catch(() => null);

    await query(`
      CREATE INDEX IF NOT EXISTS idx_notifications_user_unread
      ON public.notifications (user_id, read_at, created_at DESC)
    `).catch(() => null);
  } catch (e: any) {
    console.warn("[notifications] ensure table skipped:", e?.message || e);
  }
}

export async function createNotification(args: {
  userId: string;
  kind?: string;
  title: string;
  body?: string | null;
  actionUrl?: string | null;
  actionLabel?: string | null;
  meta?: Record<string, any> | null;
}) {
  await ensureNotificationsTable();
  const result = await query(
    `
    INSERT INTO public.notifications
      (user_id, kind, title, body, action_url, action_label, meta)
    VALUES ($1::uuid,$2,$3,$4,$5,$6,$7)
    RETURNING *
    `,
    [
      String(args.userId || ""),
      String(args.kind || "info").trim(),
      String(args.title || "").trim(),
      args.body || null,
      args.actionUrl || null,
      args.actionLabel || null,
      args.meta || {},
    ],
  );
  return result.rows?.[0] || null;
}

router.get("/", requireAuth, async (req: any, res) => {
  try {
    await ensureNotificationsTable();
    const limit = Math.max(
      1,
      Math.min(Number(req.query?.limit || 20) || 20, 100),
    );
    const result = await query(
      `
      SELECT *
      FROM public.notifications
      WHERE user_id = $1::uuid
      ORDER BY created_at DESC
      LIMIT $2
      `,
      [String(req.user?.id || ""), limit],
    );
    return res.json({ ok: true, items: result.rows || [] });
  } catch (e: any) {
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to load notifications" });
  }
});

router.get("/unread-count", requireAuth, async (req: any, res) => {
  try {
    await ensureNotificationsTable();
    const result = await query(
      `
      SELECT COUNT(*)::int AS count
      FROM public.notifications
      WHERE user_id = $1::uuid
        AND read_at IS NULL
      `,
      [String(req.user?.id || "")],
    );
    return res.json({ ok: true, count: result.rows?.[0]?.count || 0 });
  } catch (e: any) {
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "Failed to load unread count" });
  }
});

router.post("/:id/read", requireAuth, async (req: any, res) => {
  try {
    await ensureNotificationsTable();
    const result = await query(
      `
      UPDATE public.notifications
      SET read_at = COALESCE(read_at, NOW())
      WHERE id = $1::uuid
        AND user_id = $2::uuid
      RETURNING *
      `,
      [String(req.params?.id || ""), String(req.user?.id || "")],
    );

    await writeAudit({
      user_id: String(req.user?.id || ""),
      action: "notification_read",
      entity: "notification",
      entity_id: String(req.params?.id || ""),
      client_id: null,
      meta: {},
      ip: req.ip,
    });

    return res.json({ ok: true, item: result.rows?.[0] || null });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to mark notification as read",
    });
  }
});

router.post("/read-all", requireAuth, async (req: any, res) => {
  try {
    await ensureNotificationsTable();
    await query(
      `
      UPDATE public.notifications
      SET read_at = COALESCE(read_at, NOW())
      WHERE user_id = $1::uuid
        AND read_at IS NULL
      `,
      [String(req.user?.id || "")],
    );

    await writeAudit({
      user_id: String(req.user?.id || ""),
      action: "notifications_read_all",
      entity: "notification",
      entity_id: null,
      client_id: null,
      meta: {},
      ip: req.ip,
    });

    return res.json({ ok: true });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "Failed to mark all notifications as read",
    });
  }
});

export default router;
