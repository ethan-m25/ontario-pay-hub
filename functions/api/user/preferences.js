import { getSession } from "../../_lib/auth.js";
import { badRequest, json, readJson, unauthorized } from "../../_lib/http.js";

export async function onRequestGet(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  const row = await context.env.DB.prepare(
    "SELECT category, region, salary_min FROM preferences WHERE user_id = ?1",
  )
    .bind(session.user.id)
    .first();
  return json({
    ok: true,
    preferences: {
      cat: row?.category || "",
      region: row?.region || "",
      salMin: row?.salary_min || 0,
    },
  });
}

export async function onRequestPut(context) {
  const session = await getSession(context);
  if (!session) return unauthorized();
  const body = await readJson(context.request);
  if (!body) return badRequest("Invalid preferences payload.");
  const cat = body.cat || "";
  const region = body.region || "";
  const salMin = Number(body.salMin) || 0;
  await context.env.DB.prepare(
    `INSERT INTO preferences (user_id, category, region, salary_min, updated_at)
     VALUES (?1, ?2, ?3, ?4, ?5)
     ON CONFLICT(user_id) DO UPDATE SET
       category = excluded.category,
       region = excluded.region,
       salary_min = excluded.salary_min,
       updated_at = excluded.updated_at`,
  )
    .bind(session.user.id, cat, region, salMin, new Date().toISOString())
    .run();
  return json({ ok: true, preferences: { cat, region, salMin } });
}
