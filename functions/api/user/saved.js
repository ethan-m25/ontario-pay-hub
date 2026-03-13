import { getSession } from "../../_lib/auth.js";
import { badRequest, json, readJson, unauthorized } from "../../_lib/http.js";

export async function onRequestGet(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  const rows = await context.env.DB.prepare(
    "SELECT job_id FROM saved_jobs WHERE user_id = ?1 ORDER BY created_at DESC",
  )
    .bind(session.user.id)
    .all();
  return json({ ok: true, saved: (rows.results || []).map((row) => row.job_id) });
}

export async function onRequestPost(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  const body = await readJson(context.request);
  const jobId = Number(body?.jobId);
  if (!jobId) return badRequest("Missing jobId.");
  await context.env.DB.prepare(
    `INSERT INTO saved_jobs (user_id, job_id, created_at)
     VALUES (?1, ?2, ?3)
     ON CONFLICT(user_id, job_id) DO NOTHING`,
  )
    .bind(session.user.id, jobId, new Date().toISOString())
    .run();
  return onRequestGet(context);
}

export async function onRequestDelete(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  const body = await readJson(context.request);
  const jobId = Number(body?.jobId);
  if (jobId) {
    await context.env.DB.prepare("DELETE FROM saved_jobs WHERE user_id = ?1 AND job_id = ?2")
      .bind(session.user.id, jobId)
      .run();
  } else {
    await context.env.DB.prepare("DELETE FROM saved_jobs WHERE user_id = ?1").bind(session.user.id).run();
  }
  return onRequestGet(context);
}
