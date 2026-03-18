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

import { runWhmcsSyncOnce, startWhmcsAutoSync } from "./jobs/whmcsSync.js";

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

async function handleWhmcsRunOnce(
  _req: express.Request,
  res: express.Response,
) {
  try {
    const result = await runWhmcsSyncOnce({
      trigger: "manual",
      initiatedBy: "legacy-route",
    });
    return res.json({ ok: true, result });
  } catch (e: any) {
    console.error("[sync] run-once error:", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "sync failed" });
  }
}

// Keep the original route for backward compatibility.
app.post("/sync/whmcs/run-once", handleWhmcsRunOnce);

// Add the simpler alias used by the frontend sync action.
app.post("/sync/whmcs/run", handleWhmcsRunOnce);

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
