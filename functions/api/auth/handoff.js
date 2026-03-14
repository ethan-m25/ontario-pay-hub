import { createSession, consumeBrowserHandoff, getBaseUrl, sanitizeRedirect } from "../../_lib/auth.js";

function page(title, body, href, tone = "warning") {
  const accent = tone === "success" ? "#10b981" : "#f97316";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>${title}</title>
    <style>
      :root { color-scheme: dark; }
      body { margin:0; min-height:100vh; display:grid; place-items:center; background:#101318; color:#f3f4f6; font:16px/1.6 ui-sans-serif,system-ui,-apple-system,sans-serif; }
      .card { width:min(92vw,540px); border:1px solid rgba(249,115,22,.28); background:#171b21; border-radius:20px; padding:28px 24px; box-shadow:0 24px 60px rgba(0,0,0,.35); }
      .kicker { color:${accent}; text-transform:uppercase; letter-spacing:.14em; font-size:11px; margin-bottom:10px; }
      h1 { margin:0 0 10px; font:700 30px/1.08 Georgia,serif; }
      p { margin:0 0 14px; color:#d1d5db; }
      a.button { display:inline-block; margin-top:10px; padding:12px 16px; border-radius:12px; background:${accent}; color:#08130f; text-decoration:none; font-weight:700; }
    </style>
  </head>
  <body>
    <div class="card">
      <div class="kicker">Ontario Pay Hub</div>
      <h1>${title}</h1>
      <p>${body}</p>
      <a class="button" href="${href}">Continue</a>
    </div>
  </body>
</html>`;
}

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const token = url.searchParams.get("token") || "";
  const baseUrl = getBaseUrl(context.request, context.env);
  if (!token) {
    return new Response(page("That browser handoff link is invalid.", "Request a fresh sign-in link and try again.", `${baseUrl}/?auth=email-expired&openAuth=1`), {
      status: 400,
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  }
  const handoff = await consumeBrowserHandoff(context, token);
  if (!handoff) {
    return new Response(page("That browser handoff link has expired.", "Use the same email to request a new sign-in link.", `${baseUrl}/?auth=email-expired&openAuth=1`), {
      status: 400,
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  }
  const headers = await createSession(context, handoff.user_id);
  const redirectTo = `${baseUrl}${sanitizeRedirect(handoff.redirect_to || "/")}?auth=email`;
  headers.set("Location", redirectTo);
  return new Response(null, { status: 302, headers });
}
