import jwt from "jsonwebtoken";

export type AuthUser = {
  id: string;
  email: string;
  name: string;
  role:
    | "admin"
    | "operations"
    | "finance"
    | "tech"
    | "staff"
    | "viewer"
    | string;
};

declare global {
  // eslint-disable-next-line no-var
  var __nlmAuthUser: AuthUser | null;
}

function normalizeRole(value: any) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

export function getTokenFromReq(req: any): string | null {
  const h = req.headers?.authorization || "";
  if (typeof h === "string" && h.toLowerCase().startsWith("bearer ")) {
    return h.slice(7).trim();
  }
  return null;
}

export function getAuthUserFromReq(req: any): AuthUser | null {
  const token = getTokenFromReq(req);
  if (!token) return null;
  try {
    const secret = process.env.JWT_SECRET || "";
    const decoded = jwt.verify(token, secret) as AuthUser;
    return decoded || null;
  } catch {
    return null;
  }
}

export function hasAnyRole(user: AuthUser | null | undefined, roles: string[]) {
  const role = normalizeRole(user?.role);
  return roles.map(normalizeRole).includes(role);
}

export function requireAuth(req: any, res: any, next: any) {
  const decoded = getAuthUserFromReq(req);
  if (!decoded) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  req.user = decoded;
  return next();
}

export function optionalAuth(req: any, _res: any, next: any) {
  const decoded = getAuthUserFromReq(req);
  if (decoded) req.user = decoded;
  next();
}

export function requireRole(roles: string[]) {
  return (req: any, res: any, next: any) => {
    const user = req.user as AuthUser | undefined;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    if (!hasAnyRole(user, roles)) {
      return res.status(403).json({ ok: false, error: "Forbidden" });
    }
    next();
  };
}
