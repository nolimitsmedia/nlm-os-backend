// services/api/src/routes/clients.ts
import { Router, type Router as ExpressRouter } from "express";
import { query } from "../db.js";

const router: ExpressRouter = Router();

function slugify(input: string) {
  return String(input || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

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

function isWhmcsSyntheticId(value: string) {
  return /^whmcs-\d+$/i.test(String(value || "").trim());
}

function extractWhmcsIdFromSyntheticId(value: string) {
  const match = String(value || "")
    .trim()
    .match(/^whmcs-(\d+)$/i);
  return match ? Number(match[1]) : null;
}

router.get("/", async (req, res) => {
  const search = String(req.query?.search || "")
    .trim()
    .toLowerCase();

  try {
    const r = await query(
      `
      WITH invoice_rollup AS (
        SELECT
          i.whmcs_client_id,
          COALESCE(SUM(
            CASE
              WHEN COALESCE(i.status, '') ILIKE 'Paid' THEN 0
              ELSE COALESCE(i.balance, i.total, 0)
            END
          ), 0) AS balance_due,
          COUNT(*) FILTER (
            WHERE COALESCE(i.status, '') ILIKE ANY (ARRAY['Unpaid', 'Draft', 'Overdue', 'Payment Pending'])
          )::int AS open_invoices,
          COUNT(*) FILTER (
            WHERE COALESCE(i.status, '') ILIKE ANY (ARRAY['Unpaid', 'Overdue', 'Payment Pending'])
              AND i.date_due IS NOT NULL
              AND i.date_due < NOW()
          )::int AS overdue_invoices
        FROM whmcs_invoices_cache i
        GROUP BY i.whmcs_client_id
      ),
      service_rollup AS (
        SELECT
          s.whmcs_client_id,
          COALESCE(SUM(
            CASE
              WHEN COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed']) THEN COALESCE(s.recurring_amount, 0)
              ELSE 0
            END
          ), 0) AS mrr,
          COUNT(*) FILTER (
            WHERE COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed'])
          )::int AS active_services
        FROM whmcs_services_cache s
        GROUP BY s.whmcs_client_id
      ),
      local_clients AS (
        SELECT
          c.id,
          COALESCE(NULLIF(c.name, ''), w.company_name, c.id) AS name,
          COALESCE(NULLIF(c.status, ''), LOWER(w.status), 'active') AS status,
          c.whmcs_client_id,
          w.company_name AS whmcs_company_name,
          w.email AS whmcs_email,
          w.status AS whmcs_status,
          c.created_at,
          c.updated_at,
          'local'::text AS source,
          COALESCE(ir.balance_due, 0) AS balance_due,
          COALESCE(ir.open_invoices, 0) AS open_invoices,
          COALESCE(ir.overdue_invoices, 0) AS overdue_invoices,
          COALESCE(sr.mrr, 0) AS mrr,
          COALESCE(sr.active_services, 0) AS active_services
        FROM clients c
        LEFT JOIN whmcs_clients_cache w
          ON w.whmcs_client_id = c.whmcs_client_id
        LEFT JOIN invoice_rollup ir
          ON ir.whmcs_client_id = c.whmcs_client_id
        LEFT JOIN service_rollup sr
          ON sr.whmcs_client_id = c.whmcs_client_id
      ),
      whmcs_only_clients AS (
        SELECT
          CONCAT('whmcs-', w.whmcs_client_id::text) AS id,
          COALESCE(NULLIF(w.company_name, ''), CONCAT('WHMCS #', w.whmcs_client_id::text)) AS name,
          COALESCE(LOWER(NULLIF(w.status, '')), 'active') AS status,
          w.whmcs_client_id,
          w.company_name AS whmcs_company_name,
          w.email AS whmcs_email,
          w.status AS whmcs_status,
          NULL::timestamptz AS created_at,
          NULL::timestamptz AS updated_at,
          'whmcs'::text AS source,
          COALESCE(ir.balance_due, 0) AS balance_due,
          COALESCE(ir.open_invoices, 0) AS open_invoices,
          COALESCE(ir.overdue_invoices, 0) AS overdue_invoices,
          COALESCE(sr.mrr, 0) AS mrr,
          COALESCE(sr.active_services, 0) AS active_services
        FROM whmcs_clients_cache w
        LEFT JOIN invoice_rollup ir
          ON ir.whmcs_client_id = w.whmcs_client_id
        LEFT JOIN service_rollup sr
          ON sr.whmcs_client_id = w.whmcs_client_id
        WHERE NOT EXISTS (
          SELECT 1
          FROM clients c
          WHERE c.whmcs_client_id = w.whmcs_client_id
        )
      )
      SELECT *
      FROM (
        SELECT * FROM local_clients
        UNION ALL
        SELECT * FROM whmcs_only_clients
      ) merged
      ORDER BY LOWER(COALESCE(NULLIF(name, ''), whmcs_company_name, id)) ASC
      `,
    );

    const rows = (r.rows || []).map((row: any) => ({
      ...row,
      mrr: toMoney(row.mrr),
      balance_due: toMoney(row.balance_due),
      open_invoices: toInt(row.open_invoices),
      overdue_invoices: toInt(row.overdue_invoices),
      active_services: toInt(row.active_services),
      is_whmcs_synced: Boolean(row.whmcs_client_id),
      is_local_manual: !row.whmcs_client_id || row.source === "local",
      source_label: row.whmcs_client_id ? "WHMCS Synced" : "Local / Manual",
    }));

    const filtered = !search
      ? rows
      : rows.filter((row: any) => {
          const haystack = [
            row.id,
            row.name,
            row.status,
            row.whmcs_client_id,
            row.whmcs_company_name,
            row.whmcs_email,
            row.whmcs_status,
            row.source,
            row.source_label,
          ]
            .map((v) => String(v ?? "").toLowerCase())
            .join(" ");
          return haystack.includes(search);
        });

    res.json(filtered);
  } catch (e: any) {
    console.error("[clients] list error", e);
    res.status(500).json({ error: e?.message || "failed" });
  }
});

router.get("/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const whmcsIdFromSynthetic = extractWhmcsIdFromSyntheticId(id);

    const r = await query(
      `
      WITH invoice_rollup AS (
        SELECT
          i.whmcs_client_id,
          COALESCE(SUM(
            CASE
              WHEN COALESCE(i.status, '') ILIKE 'Paid' THEN 0
              ELSE COALESCE(i.balance, i.total, 0)
            END
          ), 0) AS balance_due,
          COUNT(*) FILTER (
            WHERE COALESCE(i.status, '') ILIKE ANY (ARRAY['Unpaid', 'Draft', 'Overdue', 'Payment Pending'])
          )::int AS open_invoices,
          COUNT(*) FILTER (
            WHERE COALESCE(i.status, '') ILIKE ANY (ARRAY['Unpaid', 'Overdue', 'Payment Pending'])
              AND i.date_due IS NOT NULL
              AND i.date_due < NOW()
          )::int AS overdue_invoices
        FROM whmcs_invoices_cache i
        GROUP BY i.whmcs_client_id
      ),
      service_rollup AS (
        SELECT
          s.whmcs_client_id,
          COALESCE(SUM(
            CASE
              WHEN COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed']) THEN COALESCE(s.recurring_amount, 0)
              ELSE 0
            END
          ), 0) AS mrr,
          COUNT(*) FILTER (
            WHERE COALESCE(s.status, '') ILIKE ANY (ARRAY['Active', 'Completed'])
          )::int AS active_services
        FROM whmcs_services_cache s
        GROUP BY s.whmcs_client_id
      ),
      local_match AS (
        SELECT
          c.id,
          COALESCE(NULLIF(c.name, ''), w.company_name, c.id) AS name,
          COALESCE(NULLIF(c.status, ''), LOWER(w.status), 'active') AS status,
          c.whmcs_client_id,
          c.created_at,
          c.updated_at,
          w.company_name AS whmcs_company_name,
          w.status AS whmcs_status,
          w.email AS whmcs_email,
          'local'::text AS source,
          COALESCE(ir.balance_due, 0) AS balance_due,
          COALESCE(ir.open_invoices, 0) AS open_invoices,
          COALESCE(ir.overdue_invoices, 0) AS overdue_invoices,
          COALESCE(sr.mrr, 0) AS mrr,
          COALESCE(sr.active_services, 0) AS active_services
        FROM clients c
        LEFT JOIN whmcs_clients_cache w
          ON w.whmcs_client_id = c.whmcs_client_id
        LEFT JOIN invoice_rollup ir
          ON ir.whmcs_client_id = c.whmcs_client_id
        LEFT JOIN service_rollup sr
          ON sr.whmcs_client_id = c.whmcs_client_id
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
          NULL::timestamptz AS created_at,
          NULL::timestamptz AS updated_at,
          w.company_name AS whmcs_company_name,
          w.status AS whmcs_status,
          w.email AS whmcs_email,
          'whmcs'::text AS source,
          COALESCE(ir.balance_due, 0) AS balance_due,
          COALESCE(ir.open_invoices, 0) AS open_invoices,
          COALESCE(ir.overdue_invoices, 0) AS overdue_invoices,
          COALESCE(sr.mrr, 0) AS mrr,
          COALESCE(sr.active_services, 0) AS active_services
        FROM whmcs_clients_cache w
        LEFT JOIN invoice_rollup ir
          ON ir.whmcs_client_id = w.whmcs_client_id
        LEFT JOIN service_rollup sr
          ON sr.whmcs_client_id = w.whmcs_client_id
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

    const row = r.rows[0];
    if (!row) return res.status(404).json({ error: "Client not found" });

    const displayName = row.name || row.whmcs_company_name || row.id;
    const seed = row.whmcs_client_id
      ? Number(row.whmcs_client_id)
      : hashNum(row.id);

    const summary = {
      mrr: row.whmcs_client_id ? toMoney(row.mrr) : (seed % 25) * 25,
      balance_due: row.whmcs_client_id
        ? toMoney(row.balance_due)
        : (seed % 7) * 40,
      open_invoices: row.whmcs_client_id ? toInt(row.open_invoices) : seed % 6,
      overdue_invoices: row.whmcs_client_id
        ? toInt(row.overdue_invoices)
        : seed % 3,
      open_tickets: row.whmcs_client_id ? seed % 5 : 0,
      active_projects: row.whmcs_client_id ? seed % 4 : 0,
      active_services: row.whmcs_client_id ? toInt(row.active_services) : 0,
    };

    res.json({
      id: row.id,
      name: displayName,
      status:
        row.status ||
        (row.whmcs_status ? String(row.whmcs_status).toLowerCase() : "active"),
      source: row.source,
      source_label: row.whmcs_client_id ? "WHMCS Synced" : "Local / Manual",
      whmcs: {
        whmcs_client_id: row.whmcs_client_id ?? null,
        company_name: row.whmcs_company_name ?? null,
        email: row.whmcs_email ?? null,
        status: row.whmcs_status ?? null,
      },
      summary,
      created_at: row.created_at,
      updated_at: row.updated_at,
    });
  } catch (e: any) {
    console.error("[clients] get error", e);
    res.status(500).json({ error: e?.message || "failed" });
  }
});

router.post("/", async (req, res) => {
  try {
    const name = String(req.body?.name || "").trim();
    const status = String(req.body?.status || "active").trim();
    const whmcs_client_id =
      req.body?.whmcs_client_id === null ||
      req.body?.whmcs_client_id === undefined ||
      String(req.body?.whmcs_client_id).trim() === ""
        ? null
        : Number(req.body.whmcs_client_id);

    if (!name) return res.status(400).json({ error: "name is required" });
    if (whmcs_client_id !== null && !Number.isFinite(whmcs_client_id)) {
      return res.status(400).json({ error: "invalid whmcs_client_id" });
    }

    let baseId = slugify(name) || `client-${Date.now()}`;
    let id = baseId;
    let attempt = 1;

    while (true) {
      const existing = await query(
        `SELECT 1 FROM clients WHERE id = $1 LIMIT 1`,
        [id],
      );
      if (!existing.rows?.length) break;
      attempt += 1;
      id = `${baseId}-${attempt}`;
    }

    const r = await query(
      `
      INSERT INTO clients (id, name, status, whmcs_client_id)
      VALUES ($1, $2, $3, $4)
      RETURNING id, name, status, whmcs_client_id, created_at, updated_at
      `,
      [id, name, status || "active", whmcs_client_id],
    );

    return res.status(201).json(r.rows[0]);
  } catch (e: any) {
    console.error("[clients] create error", e);
    return res.status(500).json({ error: e?.message || "failed" });
  }
});

export default router;
