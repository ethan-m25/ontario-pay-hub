const ALLOWED_ORIGINS = [
  "https://ontariopayhub.fyi",
  "https://ontario-pay-hub.pages.dev",
];

export async function onRequestGet(context) {
  const referer = context.request.headers.get("Referer") || "";
  const allowed = ALLOWED_ORIGINS.some((o) => referer.startsWith(o));
  if (!allowed) {
    return new Response("[]", {
      headers: { "Content-Type": "application/json" },
    });
  }
  return context.env.ASSETS.fetch(context.request);
}
