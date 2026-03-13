import { clearSessionCookie, destroySession } from "../../_lib/auth.js";
import { json } from "../../_lib/http.js";

export async function onRequestPost(context) {
  await destroySession(context);
  const headers = clearSessionCookie();
  return json({ ok: true }, { headers });
}
