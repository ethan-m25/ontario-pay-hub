import { consumeMagicLink, createSession, getBaseUrl, sanitizeRedirect, upsertUserByEmail } from "../../../_lib/auth.js";

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function authPage({ title, body, ctaLabel, ctaHref, tone = "success", autoRedirect = false }) {
  const safeTitle = escapeHtml(title);
  const safeBody = escapeHtml(body);
  const safeLabel = escapeHtml(ctaLabel);
  const safeHref = escapeHtml(ctaHref);
  const accent = tone === "success" ? "#10b981" : "#f97316";
  const border = tone === "success" ? "rgba(16,185,129,.28)" : "rgba(249,115,22,.28)";
  const redirectScript = autoRedirect
    ? `<script>setTimeout(function(){ window.location.replace(${JSON.stringify(ctaHref)}); }, 1200);</script>`
    : "";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>${safeTitle}</title>
    <style>
      :root { color-scheme: dark; }
      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: #101318;
        color: #f3f4f6;
        font: 16px/1.6 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
      }
      .card {
        width: min(92vw, 540px);
        border: 1px solid ${border};
        background: #171b21;
        border-radius: 20px;
        padding: 28px 24px;
        box-shadow: 0 24px 60px rgba(0,0,0,.35);
      }
      .kicker {
        color: ${accent};
        text-transform: uppercase;
        letter-spacing: .14em;
        font-size: 11px;
        margin-bottom: 10px;
      }
      h1 {
        margin: 0 0 10px;
        font: 700 30px/1.08 Georgia, serif;
      }
      p { margin: 0 0 14px; color: #d1d5db; }
      .hint {
        color: #9ca3af;
        font-size: 14px;
      }
      a.button {
        display: inline-block;
        margin-top: 10px;
        padding: 12px 16px;
        border-radius: 12px;
        background: ${accent};
        color: #08130f;
        text-decoration: none;
        font-weight: 700;
      }
    </style>
  </head>
  <body>
    <div class="card">
      <div class="kicker">Ontario Pay Hub</div>
      <h1>${safeTitle}</h1>
      <p>${safeBody}</p>
      <p class="hint">Email sign-in links work once. The same browser usually stays signed in for about 30 days; a new browser or device will ask for a fresh link. If this opened inside your mail app, use your browser's Open in Browser action for the smoothest experience.</p>
      <a class="button" href="${safeHref}">${safeLabel}</a>
    </div>
    ${redirectScript}
  </body>
</html>`;
}

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const token = url.searchParams.get("token");
  const baseUrl = getBaseUrl(context.request, context.env);
  const requestNewLink = `${baseUrl}/?auth=email-expired&openAuth=1`;
  if (!token) {
    return new Response(
      authPage({
        title: "That sign-in link is invalid.",
        body: "Request a new email sign-in link from Ontario Pay Hub and try again.",
        ctaLabel: "Request a new link",
        ctaHref: requestNewLink,
        tone: "warning",
      }),
      { status: 400, headers: { "content-type": "text/html; charset=utf-8" } },
    );
  }
  const magic = await consumeMagicLink(context, token);
  if (!magic) {
    return new Response(
      authPage({
        title: "That sign-in link has already been used or expired.",
        body: "Use the same email address to request a fresh one-time link. Your account and saved jobs stay attached to that email.",
        ctaLabel: "Request a new link",
        ctaHref: requestNewLink,
        tone: "warning",
      }),
      { status: 400, headers: { "content-type": "text/html; charset=utf-8" } },
    );
  }
  const user = await upsertUserByEmail(context, {
    email: magic.email,
    name: magic.name,
    provider: "email",
  });
  const headers = await createSession(context, user.id);
  const continueTo = `${baseUrl}${sanitizeRedirect(magic.redirect_to || "/")}?auth=email`;
  headers.set("content-type", "text/html; charset=utf-8");
  return new Response(
    authPage({
      title: "You're signed in.",
      body: "Your saved jobs and preferences are ready. Using this same email later brings you back to the same account.",
      ctaLabel: "Continue to Ontario Pay Hub",
      ctaHref: continueTo,
      tone: "success",
      autoRedirect: true,
    }),
    { status: 200, headers },
  );
}
