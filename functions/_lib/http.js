export function json(data, init = {}) {
  const headers = new Headers(init.headers || {});
  if (!headers.has("content-type")) headers.set("content-type", "application/json; charset=utf-8");
  return new Response(JSON.stringify(data), { ...init, headers });
}

export function redirect(url, status = 302, headers) {
  const h = new Headers(headers || {});
  h.set("location", url);
  return new Response(null, { status, headers: h });
}

export async function readJson(request) {
  try {
    return await request.json();
  } catch {
    return null;
  }
}

export function badRequest(message, extra = {}) {
  return json({ ok: false, error: message, ...extra }, { status: 400 });
}

export function unauthorized(message = "Unauthorized") {
  return json({ ok: false, error: message }, { status: 401 });
}

export function serverError(message = "Server error") {
  return json({ ok: false, error: message }, { status: 500 });
}
