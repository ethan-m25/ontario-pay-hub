import { clearOauthState, clearPostAuthRedirect, createSession, getBaseUrl, readOauthState, readPostAuthRedirect, upsertUserByEmail } from "../../../_lib/auth.js";
import { redirect } from "../../../_lib/http.js";

async function exchangeCode(code, request, env) {
  const baseUrl = getBaseUrl(request, env);
  const body = new URLSearchParams({
    code,
    client_id: env.GOOGLE_CLIENT_ID,
    client_secret: env.GOOGLE_CLIENT_SECRET,
    redirect_uri: `${baseUrl}/api/auth/google/callback`,
    grant_type: "authorization_code",
  });
  const resp = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

async function fetchGoogleProfile(accessToken) {
  const resp = await fetch("https://openidconnect.googleapis.com/v1/userinfo", {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  const baseUrl = getBaseUrl(context.request, context.env);
  const expectedState = readOauthState(context.request);
  const redirectTo = readPostAuthRedirect(context.request);
  const headers = clearPostAuthRedirect(clearOauthState(new Headers()));
  if (!code || !state || state !== expectedState) {
    return redirect(`${baseUrl}/?auth=google-invalid`, 302, headers);
  }
  try {
    const tokens = await exchangeCode(code, context.request, context.env);
    const profile = await fetchGoogleProfile(tokens.access_token);
    if (!profile.email || profile.email_verified === false) {
      return redirect(`${baseUrl}/?auth=google-invalid`, 302, headers);
    }
    const user = await upsertUserByEmail(context, {
      email: profile.email.toLowerCase(),
      name: profile.name || profile.given_name || profile.email,
      avatarUrl: profile.picture || "",
      provider: "google",
      providerUserId: profile.sub || "",
    });
    await createSession(context, user.id, headers);
    return redirect(`${baseUrl}${redirectTo}?auth=google`, 302, headers);
  } catch {
    return redirect(`${baseUrl}/?auth=google-error`, 302, headers);
  }
}
