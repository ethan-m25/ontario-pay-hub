# Auth Setup

This repo now expects Cloudflare Pages Functions + D1 for real user accounts.

## Required pieces

1. Create a D1 database and bind it as `DB`
2. Apply [`migrations/0001_auth.sql`](/Users/clawii/ontario-pay-hub/migrations/0001_auth.sql)
3. Set these Cloudflare Pages secrets:
   - `GOOGLE_CLIENT_ID`
   - `GOOGLE_CLIENT_SECRET`
   - `RESEND_API_KEY`
4. Set these vars:
   - `APP_BASE_URL`
   - `AUTH_FROM_EMAIL`

## Google OAuth

Create a Google OAuth Web application with this redirect URI:

`https://ontariopayhub.fyi/api/auth/google/callback`

## Email magic links

The email flow uses Resend from the backend function:

- sender: `AUTH_FROM_EMAIL`
- endpoint: `POST /api/auth/email/start`

## What moved server-side

- user accounts
- sessions
- saved jobs
- default preferences

The browser now only keeps theme preference locally.
