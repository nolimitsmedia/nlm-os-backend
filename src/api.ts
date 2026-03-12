// services/api/src/db.ts
import "dotenv/config";
import { Pool, type QueryResult, type QueryResultRow } from "pg";

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is not set (services/api/.env)");
}

// basic localhost ssl handling (safe)
const isLocal =
  DATABASE_URL.includes("localhost") || DATABASE_URL.includes("127.0.0.1");

export const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: isLocal ? false : { rejectUnauthorized: false },
});

let printed = false;

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params: any[] = [],
): Promise<QueryResult<T>> {
  const res = await pool.query<T>(text, params);

  // Print once after first successful query (avoids logging secrets)
  if (!printed) {
    printed = true;
    try {
      const who = await pool.query<{
        current_user: string;
        current_database: string;
      }>(`select current_user, current_database() as current_database`);
      console.log(
        `[DB] connected as "${who.rows[0].current_user}" on "${who.rows[0].current_database}"`,
      );
    } catch {
      // ignore
    }
  }

  return res;
}
