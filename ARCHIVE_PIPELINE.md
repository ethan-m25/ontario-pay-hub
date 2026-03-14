# Job Archive Pipeline

This repository now includes a local-first archival pipeline for preserving job
posting source documents before deriving fields from them.

## Design

The archive is document-first:

- Raw HTML is the primary asset
- Clean text is a reusable derivative
- Structured fields are versioned extractions, not the source of truth
- A lightweight JSON index tracks progress and resumable state

## Local archive layout

The archive lives under `data/job-archive/` and is intentionally gitignored.

Per job:

- `jobs/<job_id>/source.json`
- `jobs/<job_id>/snapshots/<snapshot_id>/raw.html`
- `jobs/<job_id>/snapshots/<snapshot_id>/clean.txt`
- `jobs/<job_id>/snapshots/<snapshot_id>/meta.json`
- `jobs/<job_id>/extractions/<field>.v1.json`

Control-plane files:

- `index.json`
- `state/archive-run.json`
- `state/extract-run.json`

## Scripts

- `scripts/archive_job_pages.py`
  - reads `data/jobs.json`
  - captures active job pages with source URLs
  - writes resumable raw snapshots and clean text

- `scripts/archive_extract.py`
  - reads archived snapshots
  - writes versioned extraction results
  - currently supports `work_mode`

## Typical usage

Archive pages:

```bash
python3 scripts/archive_job_pages.py --limit 25
python3 scripts/archive_job_pages.py --resume
```

Extract work mode from archived pages:

```bash
python3 scripts/archive_extract.py --field work_mode --limit 25
python3 scripts/archive_extract.py --field work_mode --resume
```

## Notes

- This pipeline is local-only for now and does not update the website.
- The archive is meant to preserve future AI flexibility: raw documents remain
  available even if extraction logic changes later.
