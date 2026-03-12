// services/api/src/routes/auth.ts
import { Router } from "express";
import jwt from "jsonwebtoken";
import { query } from "../db.js";
import { verifyPassword, hashPassword } from "../utils/password.js";
import { requireAuth } from "../middleware/auth.js";

const router = Router();

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

/**
 * Optional auto-migration:
 * Set NLM_AUTO_MIGRATE=1 only if your DB user can CREATE TABLE/EXTENSION.
 * Otherwise leave it OFF and manage schema via pgAdmin.
 */
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

async function countAdmins() {
  const r = await query(`SELECT COUNT(*)::int AS n FROM public.admins`);
  return r.rows[0]?.n || 0;
}

/**
 * CREATE FIRST ADMIN (bootstrap)
 * POST /auth/admins/bootstrap
 * Body: { email, name, password }
 *
 * - If no admins exist: allowed without key
 * - If admins exist: requires header x-bootstrap-key to match ADMIN_BOOTSTRAP_KEY (if set)
 */
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

    const bootstrapKey = String(process.env.ADMIN_BOOTSTRAP_KEY || "");
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

    return res.json({ ok: true, admin: r.rows[0] });
  } catch (e) {
    console.error("[auth bootstrap]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

/**
 * LOGIN
 * POST /auth/login
 * Body: { email, password }
 */
router.post("/login", async (req, res) => {
  try {
    const email = normEmail(req.body?.email);
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({ ok: false, error: "Missing credentials" });
    }

    await ensureAdminsTable();

    const admin = await query(
      `SELECT id,email,name,role,password_hash,is_active
       FROM public.admins
       WHERE LOWER(TRIM(email)) = LOWER(TRIM($1))
       LIMIT 1`,
      [email],
    );

    if (!admin.rows.length) {
      return res.status(401).json({ ok: false, error: "Invalid login" });
    }

    const user = admin.rows[0];

    if (!user.is_active) {
      return res.status(403).json({ ok: false, error: "Account disabled" });
    }

    const ok = await verifyPassword(password, user.password_hash);
    if (!ok) {
      return res.status(401).json({ ok: false, error: "Invalid login" });
    }

    const token = jwt.sign(
      { id: user.id, email: user.email, name: user.name, role: user.role },
      mustJwtSecret(),
      { expiresIn: "7d" },
    );

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

/**
 * ME
 * GET /auth/me
 * Header: Authorization: Bearer <token>
 */
router.get("/me", requireAuth, async (req: any, res) => {
  return res.json({ ok: true, user: req.user });
});

/**
 * UPDATE MY ADMIN (email/password/name)
 * POST /auth/admins/update
 * Requires auth
 * Body: { email?, password?, name? }
 */
router.post("/admins/update", requireAuth, async (req: any, res) => {
  try {
    await ensureAdminsTable();

    const actor = req.user;
    if (!actor?.id)
      return res.status(401).json({ ok: false, error: "Unauthorized" });

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

    return res.json({ ok: true, admin: upd.rows[0] });
  } catch (e) {
    console.error("[auth admins/update]", e);
    return res.status(500).json({ ok: false, error: pickErr(e) });
  }
});

/* -----------------------------
   DEBUG (only if NLM_DEV_DEBUG_AUTH=1)
----------------------------- */
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
