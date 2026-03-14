import { ensurePreferencesSchema, getSession } from "../../_lib/auth.js";
import { badRequest, json, readJson, unauthorized } from "../../_lib/http.js";

export async function onRequestGet(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  await ensurePreferencesSchema(context);
  const row = await context.env.DB.prepare(
    "SELECT category, region, salary_min, show_archived FROM preferences WHERE user_id = ?1",
  )
    .bind(session.user.id)
    .first();
  return json({
    ok: true,
    preferences: {
      cat: row?.category || "",
      region: row?.region || "",
      salMin: row?.salary_min || 0,
      showArchived: row?.show_archived !== 0,
    },
  });
}

export async function onRequestPut(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  await ensurePreferencesSchema(context);
  const body = await readJson(context.request);
  if (!body) return badRequest("Invalid preferences payload.");
  const cat = body.cat || "";
  const region = body.region || "";
  const salMin = Number(body.salMin) || 0;
  const showArchived = body.showArchived === false ? 0 : 1;
  await context.env.DB.prepare(
    `INSERT INTO preferences (user_id, category, region, salary_min, show_archived, updated_at)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
     ON CONFLICT(user_id) DO UPDATE SET
       category = excluded.category,
       region = excluded.region,
       salary_min = excluded.salary_min,
       show_archived = excluded.show_archived,
       updated_at = excluded.updated_at`,
  )
    .bind(session.user.id, cat, region, salMin, showArchived, new Date().toISOString())
    .run();
  return json({ ok: true, preferences: { cat, region, salMin, showArchived: showArchived !== 0 } });
}
