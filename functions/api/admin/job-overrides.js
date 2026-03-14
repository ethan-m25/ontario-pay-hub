import { ensureAdminSchema, requireAdminSession } from "../../_lib/auth.js";
import { badRequest, forbidden, json, readJson, unauthorized } from "../../_lib/http.js";

const ALLOWED = new Set(["active", "archived", "hidden"]);

export async function onRequestGet(context) {
  const { error } = await requireAdminSession(context);
  if (error === "auth") return unauthorized();
  if (error === "forbidden") return forbidden("Admin access required.");
  await ensureAdminSchema(context);
  const rows = await context.env.DB.prepare(
    "SELECT job_id, status, note, updated_at, updated_by FROM job_admin_overrides ORDER BY updated_at DESC",
  ).all();
  return json({ ok: true, overrides: rows.results || [] });
}

export async function onRequestPut(context) {
  const { session, error } = await requireAdminSession(context);
  if (error === "auth") return unauthorized();
  if (error === "forbidden") return forbidden("Admin access required.");
  await ensureAdminSchema(context);

  const body = await readJson(context.request);
  const jobId = Number(body?.jobId);
  const status = String(body?.status || "").trim().toLowerCase();
  const note = String(body?.note || "").trim().slice(0, 240);

  if (!jobId) return badRequest("Missing jobId.");
  if (!ALLOWED.has(status)) return badRequest("Invalid status.");

  if (status === "active") {
    await context.env.DB.prepare("DELETE FROM job_admin_overrides WHERE job_id = ?1").bind(jobId).run();
  } else {
    await context.env.DB.prepare(
      `INSERT INTO job_admin_overrides (job_id, status, note, updated_at, updated_by)
       VALUES (?1, ?2, ?3, ?4, ?5)
       ON CONFLICT(job_id) DO UPDATE SET
         status = excluded.status,
         note = excluded.note,
         updated_at = excluded.updated_at,
         updated_by = excluded.updated_by`,
    )
      .bind(jobId, status, note, new Date().toISOString(), session.user.email)
      .run();
  }

  const rows = await context.env.DB.prepare(
    "SELECT job_id, status, note, updated_at, updated_by FROM job_admin_overrides ORDER BY updated_at DESC",
  ).all();
  return json({ ok: true, overrides: rows.results || [] });
}
