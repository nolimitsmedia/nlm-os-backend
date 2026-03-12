// services/api/src/utils/password.ts
import bcrypt from "bcryptjs";

export async function hashPassword(raw: string) {
  const salt = await bcrypt.genSalt(10);
  return bcrypt.hash(raw, salt);
}

export async function verifyPassword(raw: string, hash: string) {
  return bcrypt.compare(raw, hash);
}
