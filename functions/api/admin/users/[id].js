import { ensurePreferencesSchema, requireAdminSession } from "../../../_lib/auth.js";
import { badRequest, forbidden, json, unauthorized } from "../../../_lib/http.js";

export async function onRequestGet(context) {
  const { error } = await requireAdminSession(context);
  if (error === "auth") return unauthorized();
  if (error === "forbidden") return forbidden("Admin access required.");

  await ensurePreferencesSchema(context);
  const userId = Number(context.params?.id);
  if (!userId) return badRequest("Invalid user id.");

  const user = await context.env.DB.prepare(
    `SELECT id, email, name, provider, avatar_url, created_at, updated_at
     FROM users WHERE id = ?1`,
  ).bind(userId).first();
  if (!user) return badRequest("User not found.");

  const preferences = await context.env.DB.prepare(
    `SELECT category, region, salary_min, show_archived
     FROM preferences WHERE user_id = ?1`,
  ).bind(userId).first();

  const saved = await context.env.DB.prepare(
    `SELECT job_id, label, created_at
     FROM saved_jobs WHERE user_id = ?1
     ORDER BY created_at DESC`,
  ).bind(userId).all();

  const sessions = await context.env.DB.prepare(
    `SELECT id, created_at, last_seen_at, expires_at
     FROM sessions WHERE user_id = ?1
     ORDER BY last_seen_at DESC`,
  ).bind(userId).all();

  return json({
    ok: true,
    user,
    preferences: {
      cat: preferences?.category || "",
      region: preferences?.region || "",
      salMin: preferences?.salary_min || 0,
      showArchived: preferences?.show_archived !== 0,
    },
    saved: saved.results || [],
    sessions: sessions.results || [],
  });
}
