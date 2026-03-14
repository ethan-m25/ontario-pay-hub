import { authConfig, createMagicLink, getBaseUrl, sanitizeRedirect } from "../../../_lib/auth.js";
import { badRequest, json, serverError, readJson } from "../../../_lib/http.js";

function emailHtml(link, name) {
  const intro = name ? `Hi ${name},` : "Hi there,";
  return `
    <div style="font-family:Arial,sans-serif;line-height:1.6;color:#0f172a">
      <p>${intro}</p>
      <p>Use this secure link to sign in to Ontario Pay Hub and restore your saved jobs and preferences.</p>
      <p><a href="${link}" style="display:inline-block;padding:12px 18px;background:#111827;color:#fff;text-decoration:none;border-radius:8px">Sign in to Ontario Pay Hub</a></p>
      <p>This one-time link expires in 20 minutes. Use the same email whenever you want to sign back in to the same account.</p>
      <p style="color:#475569;font-size:14px">If this opens inside your mail app, use your browser's Open in Browser action for the smoothest sign-in experience.</p>
    </div>
  `;
}

export async function onRequestPost(context) {
  if (!authConfig(context.env).emailEnabled) {
    return serverError("Email login is not configured.");
  }
  const body = await readJson(context.request);
  const email = body?.email?.trim().toLowerCase();
  const name = (body?.name || "").trim();
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return badRequest("Please enter a valid email.");
  }
  const redirectTo = sanitizeRedirect(body?.redirectTo || "/");
  const token = await createMagicLink(context, { email, name, redirectTo });
  const baseUrl = getBaseUrl(context.request, context.env);
  const link = `${baseUrl}/api/auth/email/verify?token=${encodeURIComponent(token)}`;
  const resendResp = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${context.env.RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: context.env.AUTH_FROM_EMAIL,
      to: [email],
      subject: "Your Ontario Pay Hub sign-in link",
      html: emailHtml(link, name),
    }),
  });
  if (!resendResp.ok) {
    const text = await resendResp.text();
    return serverError(`Email delivery failed: ${text}`);
  }
  return json({ ok: true, message: "Check your inbox for a one-time sign-in link. Using the same email later signs you back into the same account." });
}
