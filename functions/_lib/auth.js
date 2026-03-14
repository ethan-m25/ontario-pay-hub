import { appendCookie, parseCookies, serializeCookie } from "./cookies.js";

const SESSION_COOKIE = "oph_session";
const OAUTH_COOKIE = "oph_google_state";
const REDIRECT_COOKIE = "oph_auth_redirect";

function isoNow() {
  return new Date().toISOString();
}

function addMinutes(minutes) {
  return new Date(Date.now() + minutes * 60 * 1000).toISOString();
}

function addDays(days) {
  return new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();
}

function randomToken(bytes = 32) {
  const arr = new Uint8Array(bytes);
  crypto.getRandomValues(arr);
  return [...arr].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export async function sha256Hex(input) {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function getBaseUrl(request, env) {
  return env.APP_BASE_URL || new URL(request.url).origin;
}

export function sanitizeRedirect(redirectTo) {
  if (!redirectTo || typeof redirectTo !== "string") return "/";
  return redirectTo.startsWith("/") ? redirectTo : "/";
}

export function authConfig(env) {
  return {
    googleEnabled: Boolean(env.GOOGLE_CLIENT_ID && env.GOOGLE_CLIENT_SECRET),
    emailEnabled: Boolean(env.RESEND_API_KEY && env.AUTH_FROM_EMAIL),
  };
}

export function adminEmails(env) {
  return String(env.ADMIN_EMAILS || "")
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
}

export function isAdminEmail(env, email) {
  if (!email) return false;
  return adminEmails(env).includes(String(email).trim().toLowerCase());
}

export async function createSession(context, userId, headers = new Headers()) {
  const rawToken = randomToken(32);
  const tokenHash = await sha256Hex(rawToken);
  const expiresAt = addDays(30);
  await context.env.DB.prepare(
    `INSERT INTO sessions (user_id, token_hash, created_at, last_seen_at, expires_at)
     VALUES (?1, ?2, ?3, ?3, ?4)`,
  )
    .bind(userId, tokenHash, isoNow(), expiresAt)
    .run();
  appendCookie(
    headers,
    serializeCookie(SESSION_COOKIE, rawToken, {
      maxAge: 60 * 60 * 24 * 30,
    }),
  );
  return headers;
}

export function clearSessionCookie(headers = new Headers()) {
  appendCookie(
    headers,
    serializeCookie(SESSION_COOKIE, "", {
      maxAge: 0,
      expires: new Date(0),
    }),
  );
  return headers;
}

export function setOauthState(headers = new Headers(), state) {
  appendCookie(
    headers,
    serializeCookie(OAUTH_COOKIE, state, {
      maxAge: 60 * 10,
    }),
  );
  return headers;
}

export function clearOauthState(headers = new Headers()) {
  appendCookie(
    headers,
    serializeCookie(OAUTH_COOKIE, "", {
      maxAge: 0,
      expires: new Date(0),
    }),
  );
  return headers;
}

export function readOauthState(request) {
  const cookies = parseCookies(request);
  return cookies[OAUTH_COOKIE] || "";
}

export function setPostAuthRedirect(headers = new Headers(), redirectTo = "/") {
  appendCookie(
    headers,
    serializeCookie(REDIRECT_COOKIE, redirectTo, {
      maxAge: 60 * 10,
      httpOnly: false,
    }),
  );
  return headers;
}

export function readPostAuthRedirect(request) {
  const cookies = parseCookies(request);
  return cookies[REDIRECT_COOKIE] || "/";
}

export function clearPostAuthRedirect(headers = new Headers()) {
  appendCookie(
    headers,
    serializeCookie(REDIRECT_COOKIE, "", {
      maxAge: 0,
      expires: new Date(0),
      httpOnly: false,
    }),
  );
  return headers;
}

export async function getSession(context) {
  const cookies = parseCookies(context.request);
  const rawToken = cookies[SESSION_COOKIE];
  if (!rawToken) return null;
  const tokenHash = await sha256Hex(rawToken);
  const row = await context.env.DB.prepare(
    `SELECT s.id, s.user_id, s.expires_at, u.email, u.name, u.avatar_url
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token_hash = ?1`,
  )
    .bind(tokenHash)
    .first();
  if (!row) return null;
  if (new Date(row.expires_at).getTime() <= Date.now()) {
    await context.env.DB.prepare("DELETE FROM sessions WHERE id = ?1").bind(row.id).run();
    return null;
  }
  await context.env.DB.prepare("UPDATE sessions SET last_seen_at = ?2 WHERE id = ?1")
    .bind(row.id, isoNow())
    .run();
  return {
    sessionId: row.id,
    user: {
      id: row.user_id,
      email: row.email,
      name: row.name,
      avatar_url: row.avatar_url || "",
    },
  };
}

export async function destroySession(context) {
  const cookies = parseCookies(context.request);
  const rawToken = cookies[SESSION_COOKIE];
  if (!rawToken) return;
  const tokenHash = await sha256Hex(rawToken);
  await context.env.DB.prepare("DELETE FROM sessions WHERE token_hash = ?1").bind(tokenHash).run();
}

export async function ensurePreferencesSchema(context) {
  try {
    await context.env.DB.prepare("ALTER TABLE preferences ADD COLUMN show_archived INTEGER NOT NULL DEFAULT 1").run();
  } catch (err) {
    if (!String(err.message || "").includes("duplicate column name")) throw err;
  }
}

export async function ensureAdminSchema(context) {
  await context.env.DB.prepare(
    `CREATE TABLE IF NOT EXISTS job_admin_overrides (
      job_id INTEGER PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'active',
      note TEXT DEFAULT '',
      updated_at TEXT NOT NULL,
      updated_by TEXT NOT NULL
    )`,
  ).run();
}

export async function requireAdminSession(context) {
  const session = await getSession(context);
  if (!session) return { session: null, error: "auth" };
  if (!isAdminEmail(context.env, session.user.email)) return { session, error: "forbidden" };
  return { session, error: null };
}

export async function getUserBundle(context, userId) {
  await ensurePreferencesSchema(context);
  const savedRows = await context.env.DB.prepare(
    "SELECT job_id, label FROM saved_jobs WHERE user_id = ?1 ORDER BY created_at DESC",
  )
    .bind(userId)
    .all();
  const prefRow = await context.env.DB.prepare(
    "SELECT category, region, salary_min, show_archived FROM preferences WHERE user_id = ?1",
  )
    .bind(userId)
    .first();
  return {
    saved: (savedRows.results || []).map((row) => row.job_id),
    savedMeta: Object.fromEntries((savedRows.results || []).map((row) => [row.job_id, { label: row.label || "" }])),
    preferences: {
      cat: prefRow?.category || "",
      region: prefRow?.region || "",
      salMin: prefRow?.salary_min || 0,
      showArchived: prefRow?.show_archived !== 0,
    },
  };
}

export async function upsertUserByEmail(context, { email, name, avatarUrl = "", provider = "email", providerUserId = "" }) {
  const existing = await context.env.DB.prepare("SELECT id, name FROM users WHERE email = ?1").bind(email).first();
  const displayName = (name || existing?.name || email.split("@")[0]).trim();
  if (existing) {
    await context.env.DB.prepare(
      `UPDATE users
       SET name = ?2, avatar_url = COALESCE(NULLIF(?3, ''), avatar_url), provider = ?4,
           provider_user_id = COALESCE(NULLIF(?5, ''), provider_user_id), updated_at = ?6
       WHERE id = ?1`,
    )
      .bind(existing.id, displayName, avatarUrl, provider, providerUserId, isoNow())
      .run();
    return { id: existing.id, email, name: displayName, avatar_url: avatarUrl };
  }
  const inserted = await context.env.DB.prepare(
    `INSERT INTO users (email, name, avatar_url, provider, provider_user_id, created_at, updated_at)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)`,
  )
    .bind(email, displayName, avatarUrl, provider, providerUserId, isoNow())
    .run();
  return { id: inserted.meta.last_row_id, email, name: displayName, avatar_url: avatarUrl };
}

export async function createMagicLink(context, { email, name = "", redirectTo = "/" }) {
  const token = randomToken(24);
  const tokenHash = await sha256Hex(token);
  await context.env.DB.prepare(
    `INSERT INTO magic_links (email, token_hash, name, redirect_to, expires_at, created_at)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6)`,
  )
    .bind(email, tokenHash, name, redirectTo, addMinutes(20), isoNow())
    .run();
  return token;
}

export async function consumeMagicLink(context, token) {
  const tokenHash = await sha256Hex(token);
  const row = await context.env.DB.prepare(
    `SELECT id, email, name, redirect_to, expires_at, consumed_at
     FROM magic_links WHERE token_hash = ?1`,
  )
    .bind(tokenHash)
    .first();
  if (!row) return null;
  if (row.consumed_at) return null;
  if (new Date(row.expires_at).getTime() <= Date.now()) return null;
  await context.env.DB.prepare("UPDATE magic_links SET consumed_at = ?2 WHERE id = ?1").bind(row.id, isoNow()).run();
  return row;
}
