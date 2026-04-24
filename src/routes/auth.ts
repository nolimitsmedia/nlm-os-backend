import crypto from "crypto";
import { Router } from "express";
import jwt from "jsonwebtoken";
import { query } from "../db.js";
import { requireAuth, requireRole } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";
import { verifyPassword, hashPassword } from "../utils/password.js";
import { sendInviteEmail, sendPasswordResetEmail } from "../services/email.js";
import { createNotification } from "./notifications.js";

const router = Router();

type IdentityTokenKind = "password_reset" | "invite";

function pickErr(e: any, fallback = "Something went wrong") {
  return (
    e?.response?.data?.error ||
    e?.response?.data?.message ||
    e?.message ||
    fallback
  );
}

function mustJwtSecret() {
  const secret = process.env.JWT_SECRET;
  if (!secret) throw new Error("JWT_SECRET not set");
  return secret;
}

function normEmail(v: any) {
  return String(v || "")
    .trim()
    .toLowerCase();
}

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function boolEnv(name: string, fallback = false) {
  const v = env(name);
  if (!v) return fallback;
  return ["1", "true", "yes", "on"].includes(v.toLowerCase());
}

function buildPublicLink(path: string, token: string) {
  const appUrl = env("APP_URL") || env("FRONTEND_URL") || env("WEB_URL");
  const suffix = `${path}?token=${encodeURIComponent(token)}`;
  if (appUrl) return `${appUrl.replace(/\/$/, "")}${suffix}`;
  return suffix;
}

function tokenHash(raw: string) {
  return crypto.createHash("sha256").update(raw).digest("hex");
}

function makeIdentityToken() {
  return crypto.randomBytes(32).toString("hex");
}

function tokenExpiry(hoursDefault: number) {
  const d = new Date();
  d.setHours(d.getHours() + hoursDefault);
  return d.toISOString();
}

function inviteRowStatus(row: any): "active" | "used" | "expired" {
  if (row?.used_at) return "used";
  const expiresAt = new Date(row?.expires_at || 0).getTime();
  if (Number.isFinite(expiresAt) && expiresAt < Date.now()) return "expired";
  return "active";
}

async function ensureAdminsTable() {
  if (process.env.NLM_AUTO_MIGRATE !== "1") return;

  try {
    await query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);
  } catch {
    // ignore if no permission
  }

  await query(`
    CREATE TABLE IF NOT EXISTS public.admins (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      email text UNIQUE NOT NULL,
      name text NOT NULL,
      role text NOT NULL DEFAULT 'admin',
      password_hash text NOT NULL,
      is_active boolean NOT NULL DEFAULT true,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
}

async function ensureIdentityTokensTable() {
  try {
    await query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);
  } catch {
    // ignore if no permission
  }

  try {
    await query(`
      CREATE TABLE IF NOT EXISTS public.auth_identity_tokens (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        admin_id uuid REFERENCES public.admins(id) ON DELETE CASCADE,
        email text NOT NULL,
        kind text NOT NULL,
        token_hash text NOT NULL UNIQUE,
        role text,
        invited_name text,
        created_by uuid,
        used_at timestamptz,
        expires_at timestamptz NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
      )
    `);
  } catch (e: any) {
    const msg = String(e?.message || "");
    if (
      e?.code === "42501" ||
      /must be owner of table/i.test(msg) ||
      /permission denied/i.test(msg)
    ) {
      console.warn("[auth] auth_identity_tokens table ensure skipped:", msg);
      return;
    }
    throw e;
  }

  const indexStatements = [
    `
    CREATE INDEX IF NOT EXISTS idx_auth_identity_tokens_lookup
    ON public.auth_identity_tokens (email, kind, used_at, expires_at)
    `,
    `
    CREATE INDEX IF NOT EXISTS idx_auth_identity_tokens_admin
    ON public.auth_identity_tokens (admin_id, kind)
    `,
  ];

  for (const sql of indexStatements) {
    try {
      await query(sql);
    } catch (e: any) {
      const msg = String(e?.message || "");
      if (
        e?.code === "42501" ||
        /must be owner of table/i.test(msg) ||
        /permission denied/i.test(msg)
      ) {
        console.warn("[auth] auth_identity_tokens index ensure skipped:", msg);
        break;
      }
      throw e;
    }
  }
}

async function countAdmins() {
  const r = await query(`SELECT COUNT(*)::int AS n FROM public.admins`);
  return r.rows[0]?.n || 0;
}

async function findAdminByEmail(email: string) {
  const r = await query(
    `SELECT id,email,name,role,password_hash,is_active,created_at,updated_at
     FROM public.admins
     WHERE LOWER(TRIM(email)) = LOWER(TRIM($1))
     LIMIT 1`,
    [email],
  );
  return r.rows[0] || null;
}

async function insertIdentityToken(args: {
  adminId?: string | null;
  email: string;
  kind: IdentityTokenKind;
  role?: string | null;
  invitedName?: string | null;
  createdBy?: string | null;
  expiresAt: string;
}) {
  await ensureIdentityTokensTable();
  const raw = makeIdentityToken();
  const hashed = tokenHash(raw);

  const r = await query(
    `INSERT INTO public.auth_identity_tokens
      (admin_id, email, kind, token_hash, role, invited_name, created_by, expires_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     RETURNING id, admin_id, email, kind, role, invited_name, created_by, expires_at, created_at`,
    [
      args.adminId || null,
      args.email,
      args.kind,
      hashed,
      args.role || null,
      args.invitedName || null,
      args.createdBy || null,
      args.expiresAt,
    ],
  );

  return { rawToken: raw, row: r.rows[0] };
}

async function findActiveIdentityToken(
  rawToken: string,
  kind: IdentityTokenKind,
) {
  await ensureIdentityTokensTable();
  const hashed = tokenHash(rawToken);
  const r = await query(
    `SELECT id, admin_id, email, kind, role, invited_name, created_by, used_at, expires_at, created_at
     FROM public.auth_identity_tokens
     WHERE token_hash = $1
       AND kind = $2
     LIMIT 1`,
    [hashed, kind],
  );

  const row = r.rows[0] || null;
  if (!row) return { row: null, status: "missing" as const };
  if (row.used_at) return { row, status: "used" as const };
  if (new Date(row.expires_at).getTime() < Date.now()) {
    return { row, status: "expired" as const };
  }
  return { row, status: "active" as const };
}

async function markIdentityTokenUsed(id: string) {
  await query(
    `UPDATE public.auth_identity_tokens
     SET used_at = NOW()
     WHERE id = $1`,
    [id],
  );
}

router.post("/admins/bootstrap", async (req, res) => {
  try {
    await ensureAdminsTable();

    const email = normEmail(req.body?.email);
    const name = String(req.body?.name || "").trim();
    const password = String(req.body?.password || "");

    if (!email || !name || !password) {
      return res.status(400).json({ ok: false, error: "Missing fields" });
    }

    const adminCount = await countAdmins();
    const bootstrapKey = env("ADMIN_BOOTSTRAP_KEY");
    const headerKey = String(req.headers["x-bootstrap-key"] || "");

    if (adminCount > 0 && bootstrapKey && bootstrapKey !== headerKey) {
      return res.status(403).json({
        ok: false,
        error: "Bootstrap disabled. Admin already exists.",
      });
    }

    const existing = await query(
      `SELECT id,email,name,role,is_active,created_at,updated_at
       FROM public.admins
       WHERE LOWER(TRIM(email)) = LOWER(TRIM($1))
       LIMIT 1`,
      [email],
    );

    if (existing.rows.length) {
      return res.json({ ok: true, admin: existing.rows[0], existed: true });
    }

    const password_hash = await hashPassword(password);
    const r = await query(
      `INSERT INTO public.admins (email,name,password_hash)
       VALUES ($1,$2,$3)
       RETURNING id,email,name,role,is_active,created_at,updated_at`,
      [email, name, password_hash],
    );

    await writeAudit({
      user_id: String(r.rows?.[0]?.id || ""),
      action: "bootstrap",
      entity: "admin",
      entity_id: String(r.rows?.[0]?.id || ""),
      client_id: null,
      meta: { email, role: r.rows?.[0]?.role || "admin" },
      ip: req.ip,
    });

    return res.json({ ok: true, admin: r.rows[0] });
  } catch (e) {
    console.error("[auth bootstrap]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.post("/login", async (req, res) => {
  try {
    const email = normEmail(req.body?.email);
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({ ok: false, error: "Missing credentials" });
    }

    await ensureAdminsTable();
    const user = await findAdminByEmail(email);

    if (!user) {
      await writeAudit({
        user_id: null,
        action: "login_failed",
        entity: "auth",
        entity_id: null,
        client_id: null,
        meta: { email, reason: "not_found" },
        ip: req.ip,
      });
      return res.status(401).json({ ok: false, error: "Invalid login" });
    }

    if (!user.is_active) {
      await writeAudit({
        user_id: String(user.id || ""),
        action: "login_blocked",
        entity: "auth",
        entity_id: String(user.id || ""),
        client_id: null,
        meta: { email, reason: "inactive" },
        ip: req.ip,
      });
      return res.status(403).json({ ok: false, error: "Account disabled" });
    }

    const ok = await verifyPassword(password, user.password_hash);
    if (!ok) {
      await writeAudit({
        user_id: String(user.id || ""),
        action: "login_failed",
        entity: "auth",
        entity_id: String(user.id || ""),
        client_id: null,
        meta: { email, reason: "password_mismatch" },
        ip: req.ip,
      });
      return res.status(401).json({ ok: false, error: "Invalid login" });
    }

    const token = jwt.sign(
      { id: user.id, email: user.email, name: user.name, role: user.role },
      mustJwtSecret(),
      { expiresIn: "7d" },
    );

    await writeAudit({
      user_id: String(user.id || ""),
      action: "login",
      entity: "auth",
      entity_id: String(user.id || ""),
      client_id: null,
      meta: { email: user.email, role: user.role },
      ip: req.ip,
    });

    return res.json({
      ok: true,
      token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
      },
    });
  } catch (e) {
    console.error("[auth login]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get("/me", requireAuth, async (req: any, res) => {
  return res.json({ ok: true, user: req.user });
});

router.post("/logout", requireAuth, async (req: any, res) => {
  await writeAudit({
    user_id: req.user?.id ? String(req.user.id) : null,
    action: "logout",
    entity: "auth",
    entity_id: req.user?.id ? String(req.user.id) : null,
    client_id: null,
    meta: { email: req.user?.email || null, role: req.user?.role || null },
    ip: req.ip,
  });
  return res.json({ ok: true });
});

router.post("/admins/update", requireAuth, async (req: any, res) => {
  try {
    await ensureAdminsTable();

    const actor = req.user;
    if (!actor?.id) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const nextEmail = req.body?.email ? normEmail(req.body.email) : "";
    const nextPassword = req.body?.password ? String(req.body.password) : "";
    const nextName = req.body?.name ? String(req.body.name).trim() : "";

    if (!nextEmail && !nextPassword && !nextName) {
      return res.status(400).json({ ok: false, error: "Nothing to update" });
    }

    const sets: string[] = [];
    const vals: any[] = [];
    let i = 1;

    if (nextEmail) {
      sets.push(`email=$${i++}`);
      vals.push(nextEmail);
    }
    if (nextName) {
      sets.push(`name=$${i++}`);
      vals.push(nextName);
    }
    if (nextPassword) {
      const password_hash = await hashPassword(nextPassword);
      sets.push(`password_hash=$${i++}`);
      vals.push(password_hash);
    }

    sets.push(`updated_at=now()`);
    vals.push(actor.id);

    const upd = await query(
      `UPDATE public.admins
       SET ${sets.join(", ")}
       WHERE id=$${i}
       RETURNING id,email,name,role,is_active,created_at,updated_at`,
      vals,
    );

    if (!upd.rows.length) {
      return res.status(404).json({ ok: false, error: "Admin not found" });
    }

    await writeAudit({
      user_id: String(actor.id || ""),
      action: "update",
      entity: "admin",
      entity_id: String(actor.id || ""),
      client_id: null,
      meta: {
        changed_email: Boolean(nextEmail),
        changed_name: Boolean(nextName),
        changed_password: Boolean(nextPassword),
      },
      ip: req.ip,
    });

    return res.json({ ok: true, admin: upd.rows[0] });
  } catch (e) {
    console.error("[auth admins/update]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get(
  "/admins",
  requireAuth,
  requireRole(["admin"]),
  async (_req: any, res) => {
    try {
      await ensureAdminsTable();

      const r = await query(
        `SELECT id,email,name,role,is_active,created_at,updated_at
         FROM public.admins
         ORDER BY created_at DESC, name ASC`,
      );

      return res.json({ ok: true, items: r.rows || [] });
    } catch (e) {
      console.error("[auth admins list]", e);
      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.patch(
  "/admins/:id",
  requireAuth,
  requireRole(["admin"]),
  async (req: any, res) => {
    try {
      await ensureAdminsTable();

      const adminId = String(req.params?.id || "").trim();
      const actorId = String(req.user?.id || "").trim();

      if (!adminId) {
        return res
          .status(400)
          .json({ ok: false, error: "Admin ID is required" });
      }

      const name =
        req.body?.name !== undefined
          ? String(req.body.name || "").trim()
          : undefined;
      const email =
        req.body?.email !== undefined ? normEmail(req.body.email) : undefined;
      const role =
        req.body?.role !== undefined
          ? String(req.body.role || "")
              .trim()
              .toLowerCase()
          : undefined;
      const isActive =
        req.body?.is_active !== undefined
          ? Boolean(req.body.is_active)
          : undefined;

      const allowedRoles = [
        "admin",
        "operations",
        "finance",
        "tech",
        "staff",
        "viewer",
      ];

      if (role !== undefined && !allowedRoles.includes(role)) {
        return res.status(400).json({ ok: false, error: "Invalid role" });
      }
      if (name !== undefined && !name) {
        return res
          .status(400)
          .json({ ok: false, error: "Full name is required" });
      }
      if (email !== undefined && !email) {
        return res.status(400).json({ ok: false, error: "Email is required" });
      }

      const existing = await query(
        `SELECT id,email,name,role,is_active
         FROM public.admins
         WHERE id = $1
         LIMIT 1`,
        [adminId],
      );

      const current = existing.rows?.[0];
      if (!current) {
        return res.status(404).json({ ok: false, error: "User not found" });
      }

      if (email && email !== String(current.email || "").toLowerCase()) {
        const duplicate = await query(
          `SELECT id FROM public.admins
           WHERE LOWER(TRIM(email)) = LOWER(TRIM($1))
             AND id <> $2
           LIMIT 1`,
          [email, adminId],
        );

        if (duplicate.rows.length) {
          return res
            .status(409)
            .json({ ok: false, error: "Email is already in use" });
        }
      }

      if (isActive === false && adminId === actorId) {
        return res
          .status(400)
          .json({ ok: false, error: "You cannot deactivate your own account" });
      }

      if (
        isActive === false ||
        (role !== undefined && current.role === "admin" && role !== "admin")
      ) {
        const activeAdmins = await query(
          `SELECT COUNT(*)::int AS n
           FROM public.admins
           WHERE role = 'admin'
             AND is_active = true
             AND id <> $1`,
          [adminId],
        );

        if (Number(activeAdmins.rows?.[0]?.n || 0) < 1) {
          return res
            .status(400)
            .json({
              ok: false,
              error: "At least one active admin must remain",
            });
        }
      }

      const sets: string[] = [];
      const vals: any[] = [];
      let i = 1;

      if (name !== undefined) {
        sets.push(`name = $${i++}`);
        vals.push(name);
      }
      if (email !== undefined) {
        sets.push(`email = $${i++}`);
        vals.push(email);
      }
      if (role !== undefined) {
        sets.push(`role = $${i++}`);
        vals.push(role);
      }
      if (isActive !== undefined) {
        sets.push(`is_active = $${i++}`);
        vals.push(isActive);
      }

      if (!sets.length) {
        return res.status(400).json({ ok: false, error: "Nothing to update" });
      }

      sets.push(`updated_at = NOW()`);
      vals.push(adminId);

      const updated = await query(
        `UPDATE public.admins
         SET ${sets.join(", ")}
         WHERE id = $${i}
         RETURNING id,email,name,role,is_active,created_at,updated_at`,
        vals,
      );

      await writeAudit({
        user_id: actorId || null,
        action: "update",
        entity: "admin_user",
        entity_id: adminId,
        client_id: null,
        meta: { changed_fields: Object.keys(req.body || {}) },
        ip: req.ip,
      });

      return res.json({ ok: true, user: updated.rows[0] });
    } catch (e: any) {
      console.error("[auth admins update user]", e);

      if (e?.code === "23505") {
        return res
          .status(409)
          .json({ ok: false, error: "Email is already in use" });
      }

      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.delete(
  "/admins/:id",
  requireAuth,
  requireRole(["admin"]),
  async (req: any, res) => {
    try {
      await ensureAdminsTable();

      const adminId = String(req.params?.id || "").trim();
      const actorId = String(req.user?.id || "").trim();

      if (!adminId) {
        return res
          .status(400)
          .json({ ok: false, error: "Admin ID is required" });
      }
      if (adminId === actorId) {
        return res
          .status(400)
          .json({ ok: false, error: "You cannot delete your own account" });
      }

      const existing = await query(
        `SELECT id,email,name,role,is_active
         FROM public.admins
         WHERE id = $1
         LIMIT 1`,
        [adminId],
      );

      const current = existing.rows?.[0];
      if (!current) {
        return res.status(404).json({ ok: false, error: "User not found" });
      }

      if (current.role === "admin" && current.is_active) {
        const activeAdmins = await query(
          `SELECT COUNT(*)::int AS n
           FROM public.admins
           WHERE role = 'admin'
             AND is_active = true
             AND id <> $1`,
          [adminId],
        );

        if (Number(activeAdmins.rows?.[0]?.n || 0) < 1) {
          return res
            .status(400)
            .json({
              ok: false,
              error: "At least one active admin must remain",
            });
        }
      }

      await query(`DELETE FROM public.admins WHERE id = $1`, [adminId]);

      await writeAudit({
        user_id: actorId || null,
        action: "delete",
        entity: "admin_user",
        entity_id: adminId,
        client_id: null,
        meta: { email: current.email, role: current.role, name: current.name },
        ip: req.ip,
      });

      return res.json({ ok: true });
    } catch (e) {
      console.error("[auth admins delete user]", e);
      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.post("/forgot-password", async (req, res) => {
  try {
    await ensureAdminsTable();
    await ensureIdentityTokensTable();

    const email = normEmail(req.body?.email);
    if (!email) {
      return res.status(400).json({ ok: false, error: "Email is required" });
    }

    const user = await findAdminByEmail(email);
    if (!user || !user.is_active) {
      await writeAudit({
        user_id: null,
        action: "password_reset_requested_missing",
        entity: "auth",
        entity_id: null,
        client_id: null,
        meta: { email },
        ip: req.ip,
      });

      return res.status(404).json({
        ok: false,
        error: "Email address not found in our database.",
      });
    }

    const resetHours = Number(env("AUTH_RESET_TOKEN_HOURS", "2")) || 2;
    const created = await insertIdentityToken({
      adminId: String(user.id),
      email,
      kind: "password_reset",
      expiresAt: tokenExpiry(resetHours),
    });

    const link = buildPublicLink("/reset-password", created.rawToken);

    let emailResult: any = null;
    try {
      emailResult = await sendPasswordResetEmail({
        to: email,
        name: String(user.name || "").trim(),
        resetUrl: link,
      });
    } catch (emailError: any) {
      console.warn(
        "[auth forgot-password email]",
        emailError?.message || emailError,
      );
      return res.status(500).json({
        ok: false,
        error: "Failed to send reset email. Please try again.",
      });
    }

    await writeAudit({
      user_id: String(user.id),
      action: "password_reset_requested",
      entity: "auth",
      entity_id: String(user.id),
      client_id: null,
      meta: {
        email,
        email_sent: Boolean(emailResult?.ok),
      },
      ip: req.ip,
    });

    return res.json({
      ok: true,
      message: "A password reset link has been sent to your email address.",
      email_sent: Boolean(emailResult?.ok),
    });
  } catch (e) {
    console.error("[auth forgot-password]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get("/reset-password/inspect", async (req, res) => {
  try {
    const token = String(req.query?.token || "").trim();
    if (!token) {
      return res.status(400).json({ ok: false, error: "Token is required" });
    }

    const found = await findActiveIdentityToken(token, "password_reset");
    if (found.status !== "active") {
      return res.json({
        ok: true,
        valid: false,
        status: found.status,
        email: found.row?.email || null,
      });
    }

    return res.json({
      ok: true,
      valid: true,
      status: "active",
      email: found.row?.email || null,
      expires_at: found.row?.expires_at || null,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.post("/reset-password/complete", async (req, res) => {
  try {
    await ensureAdminsTable();
    await ensureIdentityTokensTable();

    const token = String(req.body?.token || "").trim();
    const password = String(req.body?.password || "");

    if (!token || !password) {
      return res
        .status(400)
        .json({ ok: false, error: "Token and password are required" });
    }
    if (password.length < 8) {
      return res
        .status(400)
        .json({ ok: false, error: "Password must be at least 8 characters" });
    }

    const found = await findActiveIdentityToken(token, "password_reset");
    if (found.status !== "active" || !found.row?.admin_id) {
      return res
        .status(400)
        .json({ ok: false, error: "Reset token is invalid or expired" });
    }

    const password_hash = await hashPassword(password);
    await query(
      `UPDATE public.admins
       SET password_hash = $1, updated_at = NOW(), is_active = true
       WHERE id = $2`,
      [password_hash, found.row.admin_id],
    );
    await markIdentityTokenUsed(String(found.row.id));

    await writeAudit({
      user_id: String(found.row.admin_id),
      action: "password_reset_completed",
      entity: "auth",
      entity_id: String(found.row.admin_id),
      client_id: null,
      meta: { email: found.row.email },
      ip: req.ip,
    });

    return res.json({ ok: true, message: "Password updated successfully." });
  } catch (e) {
    console.error("[auth reset-password complete]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.post(
  "/admins/invite",
  requireAuth,
  requireRole(["admin"]),
  async (req: any, res) => {
    try {
      await ensureAdminsTable();
      await ensureIdentityTokensTable();

      const email = normEmail(req.body?.email);
      const name = String(req.body?.name || "").trim();
      const role = String(req.body?.role || "staff")
        .trim()
        .toLowerCase();

      if (!email || !name) {
        return res
          .status(400)
          .json({ ok: false, error: "Email and name are required" });
      }

      const allowedRoles = [
        "admin",
        "operations",
        "finance",
        "tech",
        "staff",
        "viewer",
      ];
      if (!allowedRoles.includes(role)) {
        return res.status(400).json({ ok: false, error: "Invalid role" });
      }

      let admin = await findAdminByEmail(email);
      if (!admin) {
        const placeholderHash = await hashPassword(makeIdentityToken());
        const created = await query(
          `INSERT INTO public.admins (email, name, role, password_hash, is_active)
           VALUES ($1,$2,$3,$4,false)
           RETURNING id,email,name,role,is_active,created_at,updated_at`,
          [email, name, role, placeholderHash],
        );
        admin = created.rows[0];
      } else {
        await query(
          `UPDATE public.admins
           SET name = $1, role = $2, updated_at = NOW()
           WHERE id = $3`,
          [name, role, admin.id],
        );
      }

      const inviteHours = Number(env("AUTH_INVITE_TOKEN_HOURS", "72")) || 72;
      const createdInvite = await insertIdentityToken({
        adminId: String(admin.id),
        email,
        kind: "invite",
        role,
        invitedName: name,
        createdBy: String(req.user?.id || ""),
        expiresAt: tokenExpiry(inviteHours),
      });

      const link = buildPublicLink("/setup-account", createdInvite.rawToken);

      let emailResult: any = null;
      try {
        emailResult = await sendInviteEmail({
          to: email,
          invitedName: name,
          invitedByName: String(req.user?.name || req.user?.email || "NLM OS"),
          role,
          setupUrl: link,
        });
      } catch (emailError: any) {
        console.warn("[auth invite email]", emailError?.message || emailError);
      }

      try {
        await createNotification({
          userId: String(admin.id),
          kind: "invite",
          title: "You were invited to NLM OS",
          body: `You were invited as ${role}. Use the email link to set up your account.`,
          actionUrl: "/setup-account",
          actionLabel: "Set up account",
          meta: {
            email,
            role,
            invited_name: name,
            invited_by: String(req.user?.id || ""),
          },
        });
      } catch (notificationError: any) {
        console.warn(
          "[auth invite notification]",
          notificationError?.message || notificationError,
        );
      }

      await writeAudit({
        user_id: String(req.user?.id || ""),
        action: "invite_created",
        entity: "admin",
        entity_id: String(admin.id),
        client_id: null,
        meta: {
          email,
          role,
          name,
          email_sent: Boolean(emailResult?.ok),
        },
        ip: req.ip,
      });

      return res.status(201).json({
        ok: true,
        invite: {
          email,
          name,
          role,
          expires_at: createdInvite.row?.expires_at || null,
        },
        email_sent: Boolean(emailResult?.ok),
        dev_link: boolEnv("AUTH_RETURN_DEV_LINKS", true) ? link : undefined,
      });
    } catch (e) {
      console.error("[auth invites]", e);
      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.post(
  "/invites",
  requireAuth,
  requireRole(["admin"]),
  async (req: any, res) => {
    try {
      await ensureAdminsTable();
      await ensureIdentityTokensTable();

      const email = normEmail(req.body?.email);
      const name = String(req.body?.name || "").trim();
      const role = String(req.body?.role || "staff")
        .trim()
        .toLowerCase();

      if (!email || !name) {
        return res
          .status(400)
          .json({ ok: false, error: "Email and name are required" });
      }

      const allowedRoles = [
        "admin",
        "operations",
        "finance",
        "tech",
        "staff",
        "viewer",
      ];
      if (!allowedRoles.includes(role)) {
        return res.status(400).json({ ok: false, error: "Invalid role" });
      }

      let admin = await findAdminByEmail(email);
      if (!admin) {
        const placeholderHash = await hashPassword(makeIdentityToken());
        const created = await query(
          `INSERT INTO public.admins (email, name, role, password_hash, is_active)
         VALUES ($1,$2,$3,$4,false)
         RETURNING id,email,name,role,is_active,created_at,updated_at`,
          [email, name, role, placeholderHash],
        );
        admin = created.rows[0];
      } else {
        await query(
          `UPDATE public.admins SET name = $1, role = $2, updated_at = NOW() WHERE id = $3`,
          [name, role, admin.id],
        );
      }

      const inviteHours = Number(env("AUTH_INVITE_TOKEN_HOURS", "72")) || 72;
      const createdInvite = await insertIdentityToken({
        adminId: String(admin.id),
        email,
        kind: "invite",
        role,
        invitedName: name,
        createdBy: String(req.user?.id || ""),
        expiresAt: tokenExpiry(inviteHours),
      });
      const link = buildPublicLink("/setup-account", createdInvite.rawToken);

      await writeAudit({
        user_id: String(req.user?.id || ""),
        action: "invite_created",
        entity: "admin",
        entity_id: String(admin.id),
        client_id: null,
        meta: { email, role, name },
        ip: req.ip,
      });

      return res.status(201).json({
        ok: true,
        invite: {
          email,
          name,
          role,
          expires_at: createdInvite.row?.expires_at || null,
        },
        dev_link: boolEnv("AUTH_RETURN_DEV_LINKS", true) ? link : undefined,
      });
    } catch (e) {
      console.error("[auth invites]", e);
      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.get("/invites/inspect", async (req, res) => {
  try {
    const token = String(req.query?.token || "").trim();
    if (!token) {
      return res.status(400).json({ ok: false, error: "Token is required" });
    }

    const found = await findActiveIdentityToken(token, "invite");
    if (found.status !== "active") {
      return res.json({
        ok: true,
        valid: false,
        status: found.status,
        email: found.row?.email || null,
      });
    }

    return res.json({
      ok: true,
      valid: true,
      status: "active",
      email: found.row?.email || null,
      name: found.row?.invited_name || null,
      role: found.row?.role || null,
      expires_at: found.row?.expires_at || null,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.post("/invites/accept", async (req, res) => {
  try {
    await ensureAdminsTable();
    await ensureIdentityTokensTable();

    const token = String(req.body?.token || "").trim();
    const password = String(req.body?.password || "");
    const name = String(req.body?.name || "").trim();

    if (!token || !password) {
      return res
        .status(400)
        .json({ ok: false, error: "Token and password are required" });
    }
    if (password.length < 8) {
      return res
        .status(400)
        .json({ ok: false, error: "Password must be at least 8 characters" });
    }

    const found = await findActiveIdentityToken(token, "invite");
    if (found.status !== "active" || !found.row?.admin_id) {
      return res
        .status(400)
        .json({ ok: false, error: "Invite token is invalid or expired" });
    }

    const password_hash = await hashPassword(password);
    await query(
      `UPDATE public.admins
       SET password_hash = $1,
           name = COALESCE(NULLIF($2, ''), name),
           role = COALESCE(NULLIF($3, ''), role),
           is_active = true,
           updated_at = NOW()
       WHERE id = $4`,
      [
        password_hash,
        name,
        String(found.row.role || "").trim(),
        found.row.admin_id,
      ],
    );
    await markIdentityTokenUsed(String(found.row.id));

    const admin = await query(
      `SELECT id,email,name,role,is_active FROM public.admins WHERE id = $1 LIMIT 1`,
      [found.row.admin_id],
    );
    const user = admin.rows[0];

    await writeAudit({
      user_id: String(found.row.admin_id),
      action: "invite_accepted",
      entity: "admin",
      entity_id: String(found.row.admin_id),
      client_id: null,
      meta: { email: found.row.email, role: found.row.role || null },
      ip: req.ip,
    });

    const jwtToken = jwt.sign(
      { id: user.id, email: user.email, name: user.name, role: user.role },
      mustJwtSecret(),
      { expiresIn: "7d" },
    );

    return res.json({ ok: true, token: jwtToken, user });
  } catch (e) {
    console.error("[auth invites accept]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get("/_debug/db", async (_req, res) => {
  try {
    if (process.env.NLM_DEV_DEBUG_AUTH !== "1")
      return res.status(404).json({ ok: false });
    const r = await query(
      `SELECT current_database() AS db, current_schema() AS schema, current_user AS db_user`,
    );
    return res.json({ ok: true, ...r.rows[0] });
  } catch (e) {
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get("/_debug/admins", async (_req, res) => {
  try {
    if (process.env.NLM_DEV_DEBUG_AUTH !== "1")
      return res.status(404).json({ ok: false });
    const r = await query(`
      SELECT
        id,
        email,
        LENGTH(email)::int AS email_len,
        LENGTH(TRIM(email))::int AS email_trim_len,
        LEFT(password_hash, 4) AS hash_prefix,
        LENGTH(password_hash)::int AS hash_len,
        is_active,
        created_at
      FROM public.admins
      ORDER BY created_at DESC
      LIMIT 50
    `);
    return res.json({ ok: true, admins: r.rows });
  } catch (e) {
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.post("/_debug/check", async (req, res) => {
  try {
    if (process.env.NLM_DEV_DEBUG_AUTH !== "1")
      return res.status(404).json({ ok: false });

    const email = normEmail(req.body?.email);
    const password = String(req.body?.password || "");

    const r = await query(
      `SELECT id,email,is_active,password_hash
       FROM public.admins
       WHERE LOWER(TRIM(email)) = LOWER(TRIM($1))
       LIMIT 1`,
      [email],
    );

    if (!r.rows.length) return res.json({ ok: true, found: false });

    const row = r.rows[0];
    const passOk = await verifyPassword(password, row.password_hash);

    return res.json({
      ok: true,
      found: true,
      id: row.id,
      email: row.email,
      is_active: row.is_active,
      password_matches: passOk,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get(
  "/admins/invites",
  requireAuth,
  requireRole(["admin"]),
  async (req: any, res) => {
    try {
      await ensureIdentityTokensTable();

      const r = await query(
        `
        SELECT id, admin_id, email, kind, role, invited_name, created_by, used_at, expires_at, created_at
        FROM public.auth_identity_tokens
        WHERE kind = 'invite'
        ORDER BY created_at DESC
        LIMIT 100
        `,
      );

      const items = (r.rows || []).map((row: any) => ({
        ...row,
        status: inviteRowStatus(row),
      }));

      return res.json({ ok: true, items });
    } catch (e) {
      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.post(
  "/admins/invite/resend",
  requireAuth,
  requireRole(["admin"]),
  async (req: any, res) => {
    try {
      await ensureAdminsTable();
      await ensureIdentityTokensTable();

      const inviteId = String(req.body?.inviteId || "").trim();
      if (!inviteId) {
        return res
          .status(400)
          .json({ ok: false, error: "Invite ID is required" });
      }

      const existingInvite = await query(
        `
        SELECT id, admin_id, email, role, invited_name, created_by, used_at, expires_at, created_at
        FROM public.auth_identity_tokens
        WHERE id = $1
          AND kind = 'invite'
        LIMIT 1
        `,
        [inviteId],
      );

      const row = existingInvite.rows?.[0];
      if (!row) {
        return res.status(404).json({ ok: false, error: "Invite not found" });
      }

      if (row.used_at) {
        return res.status(400).json({
          ok: false,
          error: "Invite already accepted and cannot be resent",
        });
      }

      const admin = row.admin_id
        ? await query(
            `SELECT id,email,name,role,is_active FROM public.admins WHERE id = $1 LIMIT 1`,
            [row.admin_id],
          )
        : { rows: [] };

      const targetAdmin = admin.rows?.[0] || null;

      const inviteHours = Number(env("AUTH_INVITE_TOKEN_HOURS", "72")) || 72;
      const createdInvite = await insertIdentityToken({
        adminId: row.admin_id || null,
        email: row.email,
        kind: "invite",
        role: row.role || "staff",
        invitedName: row.invited_name || targetAdmin?.name || "",
        createdBy: String(req.user?.id || ""),
        expiresAt: tokenExpiry(inviteHours),
      });

      const link = buildPublicLink("/setup-account", createdInvite.rawToken);

      let emailResult: any = null;
      try {
        emailResult = await sendInviteEmail({
          to: row.email,
          invitedName: row.invited_name || targetAdmin?.name || "",
          invitedByName: String(req.user?.name || req.user?.email || "NLM OS"),
          role: row.role || "staff",
          setupUrl: link,
        });
      } catch (emailError: any) {
        console.warn(
          "[auth resend invite email]",
          emailError?.message || emailError,
        );
      }

      if (row.admin_id) {
        try {
          await createNotification({
            userId: String(row.admin_id),
            kind: "invite",
            title: "Your NLM OS invite was resent",
            body: `Use the new email link to set up your account as ${row.role || "staff"}.`,
            actionUrl: "/setup-account",
            actionLabel: "Set up account",
            meta: {
              email: row.email,
              role: row.role || "staff",
              invited_name: row.invited_name || "",
              resent_by: String(req.user?.id || ""),
            },
          });
        } catch (notificationError: any) {
          console.warn(
            "[auth resend invite notification]",
            notificationError?.message || notificationError,
          );
        }
      }

      await writeAudit({
        user_id: String(req.user?.id || ""),
        action: "invite_resent",
        entity: "admin",
        entity_id: String(row.admin_id || ""),
        client_id: null,
        meta: {
          email: row.email,
          role: row.role || "staff",
          invite_id: row.id,
          email_sent: Boolean(emailResult?.ok),
        },
        ip: req.ip,
      });

      return res.json({
        ok: true,
        email_sent: Boolean(emailResult?.ok),
        dev_link: boolEnv("AUTH_RETURN_DEV_LINKS", true) ? link : undefined,
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: pickErr(e) });
    }
  },
);

router.post("/password-reset/request", async (req, res) => {
  try {
    await ensureAdminsTable();
    await ensureIdentityTokensTable();

    const email = normEmail(req.body?.email);
    if (!email) {
      return res.status(400).json({ ok: false, error: "Email is required" });
    }

    const user = await findAdminByEmail(email);
    if (!user || !user.is_active) {
      await writeAudit({
        user_id: null,
        action: "password_reset_requested_missing",
        entity: "auth",
        entity_id: null,
        client_id: null,
        meta: { email },
        ip: req.ip,
      });

      return res.status(404).json({
        ok: false,
        error: "Email address not found in our database.",
      });
    }

    const resetHours = Number(env("AUTH_RESET_TOKEN_HOURS", "2")) || 2;
    const created = await insertIdentityToken({
      adminId: String(user.id),
      email,
      kind: "password_reset",
      expiresAt: tokenExpiry(resetHours),
    });

    const link = buildPublicLink("/reset-password", created.rawToken);

    let emailResult: any = null;
    try {
      emailResult = await sendPasswordResetEmail({
        to: email,
        name: String(user.name || "").trim(),
        resetUrl: link,
      });
    } catch (emailError: any) {
      console.warn(
        "[auth password-reset email]",
        emailError?.message || emailError,
      );
      return res.status(500).json({
        ok: false,
        error: "Failed to send reset email. Please try again.",
      });
    }

    await writeAudit({
      user_id: String(user.id),
      action: "password_reset_requested",
      entity: "auth",
      entity_id: String(user.id),
      client_id: null,
      meta: {
        email,
        email_sent: Boolean(emailResult?.ok),
      },
      ip: req.ip,
    });

    return res.json({
      ok: true,
      message: "A password reset link has been sent to your email address.",
      email_sent: Boolean(emailResult?.ok),
    });
  } catch (e) {
    console.error("[auth password-reset request]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.get("/password-reset/inspect", async (req, res) => {
  try {
    const token = String(req.query?.token || "").trim();
    if (!token) {
      return res.status(400).json({ ok: false, error: "Token is required" });
    }

    const found = await findActiveIdentityToken(token, "password_reset");
    if (found.status !== "active") {
      return res.json({
        ok: true,
        valid: false,
        status: found.status,
        email: found.row?.email || null,
      });
    }

    return res.json({
      ok: true,
      valid: true,
      status: "active",
      email: found.row?.email || null,
      expires_at: found.row?.expires_at || null,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

router.post("/password-reset/complete", async (req, res) => {
  try {
    await ensureAdminsTable();
    await ensureIdentityTokensTable();

    const token = String(req.body?.token || "").trim();
    const password = String(req.body?.password || "");

    if (!token || !password) {
      return res
        .status(400)
        .json({ ok: false, error: "Token and password are required" });
    }
    if (password.length < 8) {
      return res
        .status(400)
        .json({ ok: false, error: "Password must be at least 8 characters" });
    }

    const found = await findActiveIdentityToken(token, "password_reset");
    if (found.status !== "active" || !found.row?.admin_id) {
      return res
        .status(400)
        .json({ ok: false, error: "Reset token is invalid or expired" });
    }

    const password_hash = await hashPassword(password);
    await query(
      `UPDATE public.admins
       SET password_hash = $1, updated_at = NOW(), is_active = true
       WHERE id = $2`,
      [password_hash, found.row.admin_id],
    );
    await markIdentityTokenUsed(String(found.row.id));

    await writeAudit({
      user_id: String(found.row.admin_id),
      action: "password_reset_completed",
      entity: "auth",
      entity_id: String(found.row.admin_id),
      client_id: null,
      meta: { email: found.row.email },
      ip: req.ip,
    });

    return res.json({ ok: true, message: "Password updated successfully." });
  } catch (e) {
    console.error("[auth password-reset complete]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

export default router;
