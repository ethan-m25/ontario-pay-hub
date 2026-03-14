import { requireAdminSession } from "../../_lib/auth.js";
import { forbidden, json, unauthorized } from "../../_lib/http.js";

export async function onRequestGet(context) {
  const { error } = await requireAdminSession(context);
  if (error === "auth") return unauthorized();
  if (error === "forbidden") return forbidden("Admin access required.");

  const rows = await context.env.DB.prepare(
    `SELECT
       u.id,
       u.email,
       u.name,
       u.provider,
       u.created_at,
       u.updated_at,
       MAX(s.last_seen_at) AS last_seen_at,
       COUNT(DISTINCT sj.job_id) AS saved_count
     FROM users u
     LEFT JOIN sessions s ON s.user_id = u.id
     LEFT JOIN saved_jobs sj ON sj.user_id = u.id
     GROUP BY u.id
     ORDER BY COALESCE(MAX(s.last_seen_at), u.created_at) DESC, u.created_at DESC`,
  ).all();

  return json({ ok: true, users: rows.results || [] });
}
