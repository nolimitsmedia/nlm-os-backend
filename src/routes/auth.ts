// services/api/src/routes/auth.ts
import crypto from "crypto";
import { Router } from "express";
import jwt from "jsonwebtoken";
import { query } from "../db.js";
import { requireAuth, requireRole } from "../middleware/auth.js";
import { writeAudit } from "../utils/audit.js";
import { verifyPassword, hashPassword } from "../utils/password.js";

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
  if (process.env.NLM_AUTO_MIGRATE !== "1") return;

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

  await query(`
    CREATE INDEX IF NOT EXISTS idx_auth_identity_tokens_lookup
    ON public.auth_identity_tokens (email, kind, used_at, expires_at)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_auth_identity_tokens_admin
    ON public.auth_identity_tokens (admin_id, kind)
  `);
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

router.post("/forgot-password", async (req, res) => {
  try {
    await ensureAdminsTable();
    await ensureIdentityTokensTable();

    const email = normEmail(req.body?.email);
    if (!email) {
      return res.status(400).json({ ok: false, error: "Email is required" });
    }

    const user = await findAdminByEmail(email);
    const resetHours = Number(env("AUTH_RESET_TOKEN_HOURS", "2")) || 2;
    let link: string | null = null;

    if (user?.id && user?.is_active) {
      const created = await insertIdentityToken({
        adminId: String(user.id),
        email,
        kind: "password_reset",
        expiresAt: tokenExpiry(resetHours),
      });
      link = buildPublicLink("/reset-password", created.rawToken);

      await writeAudit({
        user_id: String(user.id),
        action: "password_reset_requested",
        entity: "auth",
        entity_id: String(user.id),
        client_id: null,
        meta: { email },
        ip: req.ip,
      });
    } else {
      await writeAudit({
        user_id: null,
        action: "password_reset_requested_missing",
        entity: "auth",
        entity_id: null,
        client_id: null,
        meta: { email },
        ip: req.ip,
      });
    }

    return res.json({
      ok: true,
      message: "If that email exists, a reset link has been prepared.",
      dev_link: boolEnv("AUTH_RETURN_DEV_LINKS", true) ? link : undefined,
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

export default router;
