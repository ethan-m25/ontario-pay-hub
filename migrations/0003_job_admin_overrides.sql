CREATE TABLE IF NOT EXISTS job_admin_overrides (
  job_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'active',
  note TEXT DEFAULT '',
  updated_at TEXT NOT NULL,
  updated_by TEXT NOT NULL
);
