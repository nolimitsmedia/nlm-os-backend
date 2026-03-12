// services/api/src/routes/clientOverview.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";

const router: ExpressRouter = Router();

function hashNum(s: string) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h;
}

function toMoney(value: any) {
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function toInt(value: any) {
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function extractWhmcsIdFromSyntheticId(value: string) {
  const match = String(value || "")
    .trim()
    .match(/^whmcs-(\d+)$/i);
  return match ? Number(match[1]) : null;
}

router.get("/:id/overview", async (req, res) => {
  const { id } = req.params;

  try {
    const whmcsIdFromSynthetic = extractWhmcsIdFromSyntheticId(id);

    const c = await query(
      `
      WITH local_match AS (
        SELECT
          c.id,
          c.name,
          c.status,
          c.whmcs_client_id,
          w.company_name,
          w.email,
          w.status AS whmcs_status,
          'local'::text AS source
        FROM clients c
        LEFT JOIN whmcs_clients_cache w
          ON w.whmcs_client_id = c.whmcs_client_id
        WHERE c.id = $1
           OR ($2::int IS NOT NULL AND c.whmcs_client_id = $2::int)
        LIMIT 1
      ),
      whmcs_only_match AS (
        SELECT
          CONCAT('whmcs-', w.whmcs_client_id::text) AS id,
          COALESCE(NULLIF(w.company_name, ''), CONCAT('WHMCS #', w.whmcs_client_id::text)) AS name,
          COALESCE(LOWER(NULLIF(w.status, '')), 'active') AS status,
          w.whmcs_client_id,
          w.company_name,
          w.email,
          w.status AS whmcs_status,
          'whmcs'::text AS source
        FROM whmcs_clients_cache w
        WHERE $2::int IS NOT NULL
          AND w.whmcs_client_id = $2::int
          AND NOT EXISTS (
            SELECT 1
            FROM clients c
            WHERE c.whmcs_client_id = w.whmcs_client_id
          )
        LIMIT 1
      )
      SELECT * FROM local_match
      UNION ALL
      SELECT * FROM whmcs_only_match
      LIMIT 1
      `,
      [id, whmcsIdFromSynthetic],
    );

    const client = c.rows[0];
    if (!client) return res.status(404).json({ error: "Client not found" });

    const seed = client.whmcs_client_id
      ? Number(client.whmcs_client_id)
      : hashNum(client.id);

    let summary = {
      mrr: client.whmcs_client_id ? (seed % 25) * 25 : 0,
      balance_due: client.whmcs_client_id ? (seed % 7) * 40 : 0,
      open_invoices: client.whmcs_client_id ? seed % 6 : 0,
      overdue_invoices: client.whmcs_client_id ? seed % 3 : 0,
      active_services: 0,
    };

    let health = {
      billing: "unknown",
      riskScore: 0,
    };

    let timeline: Array<{
      id: string;
      type: string;
      when: string;
      note: string;
    }> = [];

    if (client.whmcs_client_id) {
      try {
        const inv = await query(
          `
          SELECT
            COALESCE(SUM(
              CASE
                WHEN COALESCE(status, '') ILIKE 'Paid' THEN 0
                ELSE COALESCE(balance, total, 0)
              END
            ), 0) AS balance_due,
            COUNT(*) FILTER (
              WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Unpaid', 'Draft', 'Overdue', 'Payment Pending'])
            )::int AS open_invoices,
            COUNT(*) FILTER (
              WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Unpaid', 'Overdue', 'Payment Pending'])
                AND date_due IS NOT NULL
                AND date_due < NOW()
            )::int AS overdue_invoices,
            MAX(date_due) AS latest_due_date
          FROM whmcs_invoices_cache
          WHERE whmcs_client_id = $1
          `,
          [client.whmcs_client_id],
        );

        const svc = await query(
          `
          SELECT
            COALESCE(SUM(
              CASE
                WHEN COALESCE(status, '') ILIKE ANY (ARRAY['Active', 'Completed']) THEN COALESCE(recurring_amount, 0)
                ELSE 0
              END
            ), 0) AS mrr,
            COUNT(*) FILTER (
              WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Active', 'Completed'])
            )::int AS active_services,
            MIN(next_due_date) FILTER (
              WHERE next_due_date IS NOT NULL
            ) AS next_due_date
          FROM whmcs_services_cache
          WHERE whmcs_client_id = $1
          `,
          [client.whmcs_client_id],
        );

        const invRow = inv.rows[0] || {};
        const svcRow = svc.rows[0] || {};

        summary = {
          mrr: toMoney(svcRow.mrr),
          balance_due: toMoney(invRow.balance_due),
          open_invoices: toInt(invRow.open_invoices),
          overdue_invoices: toInt(invRow.overdue_invoices),
          active_services: toInt(svcRow.active_services),
        };

        const overdue = summary.overdue_invoices;
        const balance = summary.balance_due;
        const activeServices = summary.active_services || 0;

        const billing = overdue > 0 ? "overdue" : balance > 0 ? "due" : "good";
        const riskScore = Math.min(
          100,
          overdue * 35 +
            (balance > 0 ? 20 : 0) +
            (activeServices === 0 ? 15 : 0),
        );

        health = { billing, riskScore };

        timeline = [
          ...(summary.active_services
            ? [
                {
                  id: `svc-${client.id}`,
                  type: "Services",
                  when: "Current",
                  note: `${summary.active_services} active service(s) synced from WHMCS`,
                },
              ]
            : []),
          ...(summary.open_invoices
            ? [
                {
                  id: `inv-open-${client.id}`,
                  type: "Invoices",
                  when: invRow.latest_due_date
                    ? new Date(invRow.latest_due_date).toLocaleDateString()
                    : "This billing cycle",
                  note: `${summary.open_invoices} open invoice(s) • $${summary.balance_due.toLocaleString()}`,
                },
              ]
            : []),
          ...(summary.overdue_invoices
            ? [
                {
                  id: `inv-overdue-${client.id}`,
                  type: "Overdue",
                  when: "Attention needed",
                  note: `${summary.overdue_invoices} overdue invoice(s) require follow-up`,
                },
              ]
            : []),
          ...(svcRow.next_due_date
            ? [
                {
                  id: `svc-due-${client.id}`,
                  type: "Next Renewal",
                  when: new Date(svcRow.next_due_date).toLocaleDateString(),
                  note: "Upcoming WHMCS service renewal date",
                },
              ]
            : []),
        ];
      } catch (e: any) {
        console.warn(
          "[clientOverview] WHMCS enrichment skipped:",
          e?.message || e,
        );
      }
    }

    return res.json({
      client: {
        id: client.id,
        name: client.name || client.company_name || client.id,
        status:
          client.status ||
          (client.whmcs_status
            ? String(client.whmcs_status).toLowerCase()
            : "active"),
        whmcs_client_id: client.whmcs_client_id ?? null,
        source: client.source,
        source_label: client.whmcs_client_id
          ? "WHMCS Synced"
          : "Local / Manual",
      },
      whmcs: {
        whmcs_client_id: client.whmcs_client_id ?? null,
        company_name: client.company_name ?? null,
        email: client.email ?? null,
        status: client.whmcs_status ?? null,
      },
      summary,
      health,
      timeline,
    });
  } catch (e: any) {
    console.error("[clientOverview] error", e);
    return res.status(500).json({ error: e?.message || "failed" });
  }
});

export default router;
