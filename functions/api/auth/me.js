import { authConfig, getSession, getUserBundle } from "../../_lib/auth.js";
import { json } from "../../_lib/http.js";

export async function onRequestGet(context) {
  const config = authConfig(context.env);
  const session = await getSession(context);
  if (!session) {
    return json({ ok: true, user: null, saved: [], preferences: { cat: "", region: "", salMin: 0 }, auth: config });
  }
  const bundle = await getUserBundle(context, session.user.id);
  return json({ ok: true, user: session.user, ...bundle, auth: config });
}
