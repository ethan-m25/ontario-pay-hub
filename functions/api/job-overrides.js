import { ensureAdminSchema } from "../_lib/auth.js";
import { json } from "../_lib/http.js";

export async function onRequestGet(context) {
  await ensureAdminSchema(context);
  const rows = await context.env.DB.prepare(
    "SELECT job_id, status, note, updated_at FROM job_admin_overrides ORDER BY updated_at DESC",
  ).all();
  const results = rows.results || [];
  return json({
    ok: true,
    overrides: Object.fromEntries(
      results.map((row) => [
        String(row.job_id),
        {
          status: row.status,
          note: row.note || "",
          updated_at: row.updated_at,
        },
      ]),
    ),
  });
}
