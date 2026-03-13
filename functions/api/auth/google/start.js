import { getBaseUrl, setOauthState, setPostAuthRedirect, authConfig, sanitizeRedirect } from "../../../_lib/auth.js";
import { redirect, serverError } from "../../../_lib/http.js";

function googleUrl(request, env, state) {
  const baseUrl = getBaseUrl(request, env);
  const params = new URLSearchParams({
    client_id: env.GOOGLE_CLIENT_ID,
    redirect_uri: `${baseUrl}/api/auth/google/callback`,
    response_type: "code",
    scope: "openid email profile",
    state,
    prompt: "select_account",
  });
  return `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`;
}

export async function onRequestGet(context) {
  if (!authConfig(context.env).googleEnabled) {
    return serverError("Google login is not configured.");
  }
  const redirectTo = sanitizeRedirect(new URL(context.request.url).searchParams.get("redirectTo") || "/");
  const state = crypto.randomUUID();
  const headers = setPostAuthRedirect(setOauthState(new Headers(), state), redirectTo);
  return redirect(googleUrl(context.request, context.env, state), 302, headers);
}
