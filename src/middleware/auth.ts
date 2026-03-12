// services/api/src/middleware/auth.ts
import jwt from "jsonwebtoken";

export type AuthUser = {
  id: string;
  email: string;
  name: string;
  role: "admin" | "staff" | "viewer" | string;
};

declare global {
  // eslint-disable-next-line no-var
  var __nlmAuthUser: AuthUser | null;
}

export function getTokenFromReq(req: any): string | null {
  const h = req.headers?.authorization || "";
  if (typeof h === "string" && h.toLowerCase().startsWith("bearer ")) {
    return h.slice(7).trim();
  }
  return null;
}

export function requireAuth(req: any, res: any, next: any) {
  const token = getTokenFromReq(req);
  if (!token) return res.status(401).json({ ok: false, error: "Unauthorized" });

  try {
    const secret = process.env.JWT_SECRET || "";
    const decoded = jwt.verify(token, secret) as AuthUser;
    req.user = decoded;
    return next();
  } catch {
    return res.status(401).json({ ok: false, error: "Invalid token" });
  }
}

export function optionalAuth(req: any, _res: any, next: any) {
  const token = getTokenFromReq(req);
  if (!token) return next();
  try {
    const secret = process.env.JWT_SECRET || "";
    const decoded = jwt.verify(token, secret) as AuthUser;
    req.user = decoded;
  } catch {
    // ignore
  }
  next();
}

export function requireRole(roles: string[]) {
  return (req: any, res: any, next: any) => {
    const user = req.user as AuthUser | undefined;
    if (!user)
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    if (!roles.includes(String(user.role))) {
      return res.status(403).json({ ok: false, error: "Forbidden" });
    }
    next();
  };
}
