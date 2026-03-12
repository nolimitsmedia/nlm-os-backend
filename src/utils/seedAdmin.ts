// services/api/src/utils/seedAdmin.ts
import { query } from "../db.js";
import { hashPassword } from "./password.js";

export async function seedAdminIfNeeded() {
  const email = (process.env.SEED_ADMIN_EMAIL || "").trim().toLowerCase();
  const password = (process.env.SEED_ADMIN_PASSWORD || "").trim();
  const name = (process.env.SEED_ADMIN_NAME || "Admin").trim();
  const role = (process.env.SEED_ADMIN_ROLE || "admin").trim();

  if (!email || !password) return;

  // check if any users exist
  const existing = await query<{ c: string }>(
    `SELECT COUNT(*)::text AS c FROM users`,
  );
  const count = Number(existing.rows?.[0]?.c || 0);
  if (count > 0) return;

  const pwHash = await hashPassword(password);

  await query(
    `INSERT INTO users (email, name, role, password_hash)
     VALUES ($1,$2,$3,$4)`,
    [email, name, role, pwHash],
  );

  console.log(`[AUTH] Seeded admin user: ${email} (${role})`);
}
