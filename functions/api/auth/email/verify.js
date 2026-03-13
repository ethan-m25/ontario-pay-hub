import { consumeMagicLink, createSession, getBaseUrl, upsertUserByEmail } from "../../../_lib/auth.js";
import { redirect } from "../../../_lib/http.js";

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const token = url.searchParams.get("token");
  const baseUrl = getBaseUrl(context.request, context.env);
  if (!token) {
    return redirect(`${baseUrl}/?auth=email-invalid`);
  }
  const magic = await consumeMagicLink(context, token);
  if (!magic) {
    return redirect(`${baseUrl}/?auth=email-expired`);
  }
  const user = await upsertUserByEmail(context, {
    email: magic.email,
    name: magic.name,
    provider: "email",
  });
  const headers = await createSession(context, user.id);
  return redirect(`${baseUrl}${magic.redirect_to || "/"}?auth=email`, 302, headers);
}
