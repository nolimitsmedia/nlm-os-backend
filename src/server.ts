// services/api/src/server.ts
import express from "express";
import cors from "cors";
import "dotenv/config";

import clientsRouter from "./routes/clients.js";
import clientOverviewRouter from "./routes/clientOverview.js";
import aiRouter from "./routes/ai.js";
import syncRouter from "./routes/sync.js";
import tasksRouter from "./routes/tasks.js";
import sopsRouter from "./routes/sops.js";
import authRouter from "./routes/auth.js";
import sharepointRouter from "./routes/sharepoint.js";

import { runWhmcsSyncOnce } from "./jobs/whmcsSync.js";

const app = express();

app.use(
  cors({
    origin: true,
    credentials: true,
  }),
);
app.use(express.json({ limit: "2mb" }));

/* -----------------------------
   Health
----------------------------- */
app.get("/health", (_req, res) => res.json({ ok: true }));

/* -----------------------------
   Core modules
----------------------------- */
app.use("/clients", clientsRouter);
app.use("/clients", clientOverviewRouter);
app.use("/ai", aiRouter);
app.use("/sync", syncRouter);

/* -----------------------------
   Module 3: Tasks overlay (ClickUp)
   Module 4: SharePoint (Microsoft Graph)
   Module 5: SOP reference layer
   Module 6: Auth
----------------------------- */
app.use("/tasks", tasksRouter);
app.use("/sharepoint", sharepointRouter);
app.use("/sops", sopsRouter);
app.use("/auth", authRouter);

/* -----------------------------
   Dev / manual sync trigger
----------------------------- */
app.post("/sync/whmcs/run-once", async (_req, res) => {
  try {
    const result = await runWhmcsSyncOnce();
    return res.json({ ok: true, result });
  } catch (e: any) {
    console.error("[sync] run-once error:", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "sync failed" });
  }
});

/* -----------------------------
   Fallback error handler
----------------------------- */
app.use((err: any, _req: any, res: any, _next: any) => {
  console.error("[server] unhandled:", err);
  res.status(500).json({ error: err?.message || "Server error" });
});

/* -----------------------------
   Start server (dev/prod)
----------------------------- */
const PORT = Number(process.env.PORT || 4000);
const HOST = process.env.HOST || "0.0.0.0";

if (process.env.NLM_API_NO_LISTEN !== "1") {
  app.listen(PORT, HOST, () => {
    console.log(`[api] listening on http://${HOST}:${PORT}`);
  });
}

export default app;
