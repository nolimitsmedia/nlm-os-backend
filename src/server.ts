// services/api/src/server.ts
import express from "express";
import cors from "cors";
import "dotenv/config";

import clientsRouter from "./routes/clients.js";
import clientOverviewRouter from "./routes/clientOverview.js";
import aiRouter from "./routes/ai.js";
import communicationsRouter from "./routes/communications.js";
import syncRouter from "./routes/sync.js";
import tasksRouter from "./routes/tasks.js";
import sopsRouter from "./routes/sops.js";
import authRouter from "./routes/auth.js";
import sharepointRouter from "./routes/sharepoint.js";
import auditRouter from "./routes/audit.js";
import notificationsRouter from "./routes/notifications.js";

import { startWhmcsAutoSync } from "./jobs/whmcsSync.js";

const app = express();

app.use(
  cors({
    origin: true,
    credentials: true,
  }),
);
app.use(express.json({ limit: "2mb" }));

app.get("/health", (_req, res) => res.json({ ok: true }));

app.use("/clients", clientsRouter);
app.use("/clients", clientOverviewRouter);
app.use("/ai", aiRouter);
app.use("/communications", communicationsRouter);
app.use("/sync", syncRouter);
app.use("/tasks", tasksRouter);
app.use("/sharepoint", sharepointRouter);
app.use("/sops", sopsRouter);
app.use("/auth", authRouter);
app.use("/audit", auditRouter);
app.use("/notifications", notificationsRouter);

app.use((err: any, _req: any, res: any, _next: any) => {
  console.error("[server] unhandled:", err);
  res.status(500).json({ error: err?.message || "Server error" });
});

const PORT = Number(process.env.PORT || 4000);
const HOST = process.env.HOST || "0.0.0.0";

if (process.env.NLM_API_NO_LISTEN !== "1") {
  app.listen(PORT, HOST, () => {
    console.log(`[api] listening on http://${HOST}:${PORT}`);
    startWhmcsAutoSync();
  });
}

export default app;
