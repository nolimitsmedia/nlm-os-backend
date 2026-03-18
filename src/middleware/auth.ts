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

export function verifyAuthToken(
  token: string | null | undefined,
): AuthUser | null {
  if (!token) return null;

  try {
    const secret = process.env.JWT_SECRET || "";
    if (!secret) return null;
    return jwt.verify(String(token), secret) as AuthUser;
  } catch {
    return null;
  }
}

export function getAuthUserFromReq(req: any): AuthUser | null {
  const token = getTokenFromReq(req);
  return verifyAuthToken(token);
}

export function requireAuth(req: any, res: any, next: any) {
  const token = getTokenFromReq(req);
  if (!token) return res.status(401).json({ ok: false, error: "Unauthorized" });

  const decoded = verifyAuthToken(token);
  if (!decoded) {
    return res.status(401).json({ ok: false, error: "Invalid token" });
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
    const user =
      (req.user as AuthUser | undefined) ||
      getAuthUserFromReq(req) ||
      undefined;
    if (!user)
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    if (!roles.includes(String(user.role))) {
      return res.status(403).json({ ok: false, error: "Forbidden" });
    }
    req.user = user;
    next();
  };
}
