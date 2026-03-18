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

async function getColumnSet(tableName: string) {
  const r = await query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = $1
    `,
    [tableName],
  );

  return new Set(
    (r.rows || []).map((row: any) => String(row.column_name || "").trim()),
  );
}

async function tableExists(tableName: string) {
  const r = await query(
    `
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = $1
    ) AS present
    `,
    [tableName],
  );

  return Boolean(r.rows?.[0]?.present);
}

function computeRiskBand(score: number) {
  if (score >= 70) return "high";
  if (score >= 35) return "medium";
  return "healthy";
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
          COALESCE(LOWER(NULLIF(w.status, '')), NULLIF(LOWER(c.status), ''), 'active') AS status,
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
            FROM clients c2
            WHERE c2.whmcs_client_id = w.whmcs_client_id
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

    const invoiceCols = await getColumnSet("whmcs_invoices_cache");
    const serviceCols = await getColumnSet("whmcs_services_cache");
    const tasksPresent = await tableExists("tasks");

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
      riskBand: "healthy",
      flags: [] as string[],
    };

    let timeline: Array<{
      id: string;
      type: string;
      when: string;
      note: string;
    }> = [];

    const contacts: Array<{
      label?: string;
      name?: string | null;
      email?: string | null;
      phone?: string | null;
      source?: string | null;
    }> = [];

    const services: Array<{
      id?: string | number | null;
      name?: string | null;
      status?: string | null;
      domain?: string | null;
      recurring_amount?: number;
      next_due_date?: string | null;
    }> = [];

    let taskCounts = {
      total: 0,
      open: 0,
      blocked: 0,
    };

    let serviceSummary = {
      total_services: 0,
      active_services: 0,
      next_renewal_date: null as string | null,
      services_at_risk: 0,
    };

    let billingDetails = {
      current_balance: 0,
      open_invoices: 0,
      overdue_invoices: 0,
      days_past_due: 0,
      last_payment_date: null as string | null,
      last_payment_amount: 0,
      payment_method_on_file: null as string | null,
      suspension_status: "Good",
      suspension_suggested: false,
      task_close_blocked: false,
      blocking_reason: null as string | null,
      system_of_origin: "WHMCS",
    };

    if (client.email) {
      contacts.push({
        label: "Primary Contact",
        name:
          client.company_name && String(client.company_name).trim()
            ? String(client.company_name).trim()
            : null,
        email: client.email,
        source: client.source === "whmcs" ? "WHMCS" : "Client record",
      });
    }

    if (tasksPresent) {
      const taskRes = await query(
        `
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (
            WHERE COALESCE(LOWER(status), 'open') NOT IN ('complete', 'completed', 'closed')
          )::int AS open
        FROM tasks
        WHERE client_id = $1
        `,
        [client.id],
      ).catch(() => ({ rows: [{ total: 0, open: 0 }] }) as any);

      taskCounts = {
        total: toInt(taskRes.rows?.[0]?.total),
        open: toInt(taskRes.rows?.[0]?.open),
        blocked: 0,
      };
    }

    if (client.whmcs_client_id) {
      const invoiceAgg = await query(
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
          MIN(date_due) FILTER (
            WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Unpaid', 'Overdue', 'Payment Pending'])
              AND date_due IS NOT NULL
          ) AS oldest_open_due_date,
          MAX(date_due) FILTER (
            WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Unpaid', 'Draft', 'Overdue', 'Payment Pending'])
          ) AS latest_due_date
        FROM whmcs_invoices_cache
        WHERE whmcs_client_id = $1
        `,
        [client.whmcs_client_id],
      );

      const serviceAgg = await query(
        `
        SELECT
          COUNT(*)::int AS total_services,
          COALESCE(SUM(
            CASE
              WHEN COALESCE(status, '') ILIKE ANY (ARRAY['Active', 'Completed']) THEN COALESCE(recurring_amount, 0)
              ELSE 0
            END
          ), 0) AS mrr,
          COUNT(*) FILTER (
            WHERE COALESCE(status, '') ILIKE ANY (ARRAY['Active', 'Completed'])
          )::int AS active_services,
          COUNT(*) FILTER (
            WHERE COALESCE(status, '') ILIKE 'Suspended'
          )::int AS suspended_services,
          MIN(next_due_date) FILTER (
            WHERE next_due_date IS NOT NULL
          ) AS next_due_date
        FROM whmcs_services_cache
        WHERE whmcs_client_id = $1
        `,
        [client.whmcs_client_id],
      );

      const latestPaidRow =
        invoiceCols.has("date_paid") || invoiceCols.has("datepaid")
          ? await query(
              `
              SELECT
                ${invoiceCols.has("date_paid") ? "date_paid" : "datepaid"} AS paid_at,
                COALESCE(total, balance, 0) AS paid_amount,
                ${invoiceCols.has("payment_method") ? "payment_method" : invoiceCols.has("paymentmethod") ? "paymentmethod" : "NULL::text"} AS payment_method
              FROM whmcs_invoices_cache
              WHERE whmcs_client_id = $1
                AND COALESCE(status, '') ILIKE 'Paid'
                AND ${invoiceCols.has("date_paid") ? "date_paid" : "datepaid"} IS NOT NULL
              ORDER BY ${invoiceCols.has("date_paid") ? "date_paid" : "datepaid"} DESC
              LIMIT 1
              `,
              [client.whmcs_client_id],
            ).catch(() => ({ rows: [] }) as any)
          : ({ rows: [] } as any);

      const invoiceRow = invoiceAgg.rows[0] || {};
      const serviceRow = serviceAgg.rows[0] || {};
      const paidRow = latestPaidRow.rows?.[0] || {};

      summary = {
        mrr: toMoney(serviceRow.mrr),
        balance_due: toMoney(invoiceRow.balance_due),
        open_invoices: toInt(invoiceRow.open_invoices),
        overdue_invoices: toInt(invoiceRow.overdue_invoices),
        active_services: toInt(serviceRow.active_services),
      };

      serviceSummary = {
        total_services: toInt(serviceRow.total_services),
        active_services: toInt(serviceRow.active_services),
        next_renewal_date: serviceRow.next_due_date || null,
        services_at_risk: toInt(serviceRow.suspended_services),
      };

      billingDetails = {
        current_balance: summary.balance_due,
        open_invoices: summary.open_invoices,
        overdue_invoices: summary.overdue_invoices,
        days_past_due: invoiceRow.oldest_open_due_date
          ? Math.max(
              0,
              Math.floor(
                (Date.now() -
                  new Date(invoiceRow.oldest_open_due_date).getTime()) /
                  (1000 * 60 * 60 * 24),
              ),
            )
          : 0,
        last_payment_date: paidRow.paid_at || null,
        last_payment_amount: toMoney(paidRow.paid_amount),
        payment_method_on_file: paidRow.payment_method || null,
        suspension_status:
          toInt(serviceRow.suspended_services) > 0
            ? "Suspended service detected"
            : summary.overdue_invoices > 0
              ? "Review"
              : "Good",
        suspension_suggested:
          summary.overdue_invoices > 0 && summary.balance_due > 0,
        task_close_blocked: summary.overdue_invoices > 0,
        blocking_reason:
          summary.overdue_invoices > 0
            ? "No task close if invoice unpaid"
            : null,
        system_of_origin: "WHMCS / QuickBooks Ready",
      };

      const overdue = summary.overdue_invoices;
      const balance = summary.balance_due;
      const activeServices = summary.active_services || 0;
      const suspendedServices = toInt(serviceRow.suspended_services);
      const openTasks = taskCounts.open;

      const billing = overdue > 0 ? "overdue" : balance > 0 ? "due" : "good";
      const riskScore = Math.min(
        100,
        overdue * 35 +
          (balance > 0 ? 15 : 0) +
          (activeServices === 0 ? 15 : 0) +
          suspendedServices * 20 +
          (openTasks >= 6 ? 10 : 0),
      );

      const flags = [
        ...(overdue > 0
          ? [`${overdue} overdue invoice${overdue === 1 ? "" : "s"}`]
          : []),
        ...(balance > 0
          ? [`Balance due ${summary.balance_due.toLocaleString()}`]
          : []),
        ...(suspendedServices > 0
          ? [
              `${suspendedServices} suspended service${suspendedServices === 1 ? "" : "s"}`,
            ]
          : []),
        ...(activeServices === 0 ? ["No active services"] : []),
        ...(openTasks > 0
          ? [`${openTasks} open task${openTasks === 1 ? "" : "s"}`]
          : []),
      ];

      health = {
        billing,
        riskScore,
        riskBand: computeRiskBand(riskScore),
        flags,
      };

      timeline = [
        ...(billingDetails.last_payment_date
          ? [
              {
                id: `payment-${client.id}`,
                type: "Last Payment",
                when: new Date(
                  billingDetails.last_payment_date,
                ).toLocaleDateString(),
                note: `Latest recorded payment ${billingDetails.last_payment_amount ? `• $${billingDetails.last_payment_amount.toLocaleString()}` : ""}`,
              },
            ]
          : []),
        ...(summary.open_invoices
          ? [
              {
                id: `inv-open-${client.id}`,
                type: "Invoices",
                when: invoiceRow.latest_due_date
                  ? new Date(invoiceRow.latest_due_date).toLocaleDateString()
                  : "This billing cycle",
                note: `${summary.open_invoices} open invoice(s) • $${summary.balance_due.toLocaleString()}`,
              },
            ]
          : []),
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
        ...(serviceSummary.next_renewal_date
          ? [
              {
                id: `renewal-${client.id}`,
                type: "Next Renewal",
                when: new Date(
                  serviceSummary.next_renewal_date,
                ).toLocaleDateString(),
                note: "Upcoming WHMCS service renewal date",
              },
            ]
          : []),
        ...(taskCounts.open
          ? [
              {
                id: `tasks-${client.id}`,
                type: "Tasks",
                when: "Now",
                note: `${taskCounts.open} open task(s) attached to this client`,
              },
            ]
          : []),
      ];

      const serviceNameExpr = serviceCols.has("product_name")
        ? "product_name"
        : serviceCols.has("name")
          ? "name"
          : serviceCols.has("package_name")
            ? "package_name"
            : "NULL::text";

      const serviceIdExpr = serviceCols.has("service_id")
        ? "service_id"
        : serviceCols.has("id")
          ? "id"
          : "NULL::text";

      const domainExpr = serviceCols.has("domain") ? "domain" : "NULL::text";
      const nextDueExpr = serviceCols.has("next_due_date")
        ? "next_due_date"
        : "NULL::timestamptz";

      const svcRows = await query(
        `
        SELECT
          ${serviceIdExpr} AS id,
          ${serviceNameExpr} AS name,
          status,
          ${domainExpr} AS domain,
          COALESCE(recurring_amount, 0) AS recurring_amount,
          ${nextDueExpr} AS next_due_date
        FROM whmcs_services_cache
        WHERE whmcs_client_id = $1
        ORDER BY
          CASE WHEN COALESCE(status, '') ILIKE ANY (ARRAY['Active', 'Completed']) THEN 0 ELSE 1 END,
          COALESCE(next_due_date, NOW() + interval '10 years') ASC,
          COALESCE(recurring_amount, 0) DESC
        LIMIT 6
        `,
        [client.whmcs_client_id],
      ).catch(() => ({ rows: [] }) as any);

      for (const row of svcRows.rows || []) {
        services.push({
          id: row.id ?? null,
          name: row.name || row.domain || "Service",
          status: row.status || null,
          domain: row.domain || null,
          recurring_amount: toMoney(row.recurring_amount),
          next_due_date: row.next_due_date || null,
        });
      }
    }

    res.json({
      client: {
        id: client.id,
        name: client.name,
        status: client.status,
        whmcs_client_id: client.whmcs_client_id ?? null,
        source: client.source,
        source_label: client.whmcs_client_id
          ? "WHMCS Synced"
          : "Local / Manual",
        primary_contact_name: contacts[0]?.name || null,
        primary_contact_email: contacts[0]?.email || client.email || null,
      },
      whmcs: {
        whmcs_client_id: client.whmcs_client_id ?? null,
        company_name: client.company_name ?? null,
        email: client.email ?? null,
        status: client.whmcs_status ?? null,
      },
      contacts,
      services,
      serviceSummary,
      billingDetails,
      summary,
      health,
      timeline,
      note:
        billingDetails.task_close_blocked && billingDetails.blocking_reason
          ? billingDetails.blocking_reason
          : undefined,
    });
  } catch (e: any) {
    console.error("[clientOverview] error", e);
    res.status(500).json({ error: e?.message || "failed" });
  }
});

export default router;
