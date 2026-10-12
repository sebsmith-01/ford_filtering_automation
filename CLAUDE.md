# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This project automates the weekly filtering of social media posts relating to a set of Ford vehicles. It pulls data from downloaded Excel exports and a Ford PostgreSQL database, then populates a Google Sheet with filtered, owner-tagged posts ready for human validation.

## Running the Pipeline

The main pipeline runs four scripts in sequence:

```bash
python weekly_processing/run_all.py
```

This runs: `move_files.py` → `get_ownership_database.py` → `add_facebook_names.py` → `google_sheet_editing.py` → `autovalidation.py` (all within `weekly_processing/`)

Individual scripts can be run directly from the repo root:
```bash
python weekly_processing/move_files.py
python weekly_processing/get_ownership_database.py
python weekly_processing/add_facebook_names.py
python weekly_processing/google_sheet_editing.py
python weekly_processing/autovalidation.py
```

**Prerequisites before running:**
- Download `full_data-{date}.xlsx` to `~/Downloads/` (Facebook Groups author names, comment counts and image flags now come through in this file, so the scraper export is no longer needed)
- Ensure `.env` is populated (see Environment Variables below)

## Environment Variables (`.env`)

| Variable | Purpose |
|---|---|
| `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_DATABASE` | Ford PostgreSQL database (Ford Lens) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to Google service account JSON |
| `GOOGLE_OAUTH_TOKEN_JSON` | Path to OAuth token file (auto-generated on first run) |
| `GOOGLE_OAUTH_CLIENT_JSON` | Path to OAuth client secrets JSON (Desktop app) |
| `OPENAI_API_KEY` | OpenAI API key used by `autovalidation.py` |
| `SLACK_BOT_TOKEN` | Slack bot token (`xoxb-...`) for `slack_listener.py` and `tagging_accuracy.py` (needs `chat:write` + `files:write`, bot invited to the channel) |
| `SLACK_APP_TOKEN` | Slack app-level token (`xapp-...`) for Socket Mode |
| `SLACK_TARGET_CHANNEL` | Channel ID to listen in (e.g. `C12345678`) |
| `SLACK_TARGET_USER` | User ID whose messages trigger the pipeline (e.g. `U12345678`) |

Google Sheets auth uses OAuth (not service account) — first run opens a browser for consent. Token is cached at `GOOGLE_OAUTH_TOKEN_JSON`.

## Key Files and Data Flow

- `vehicle_ids.csv` — maps `desired_vehicle_id` integers to `vehicle_name` strings; names must match Google Sheet tab names exactly
- `weekly_data/{monday_str}/` — working folder for each week's data; date is always the Monday of the current week
- `ownership_databases/ownership_database_{monday_str}.csv` — cached owner/pre-owner data from Ford DB
- `spreadsheet_links.csv` — one row per week (`week` = Monday date, `url`, `created_at`) with the URL of that week's Google Sheet, starting from 2026-10-05. `google_sheet_editing.py` writes it; recreating a week's sheet replaces that week's row. Read/write it with `get_spreadsheet_url()` / `save_spreadsheet_url()` in `helper_functions.py`

### Script responsibilities

| Script | Input | Output |
|---|---|---|
| `weekly_processing/move_files.py` | `~/Downloads/full_data-*.xlsx` | `weekly_data/{monday_str}/` |
| `weekly_processing/get_ownership_database.py` | `vehicle_ids.csv`, Ford DB | `ownership_databases/ownership_database_{monday_str}.csv` |
| `weekly_processing/add_facebook_names.py` | `weekly_data/{monday_str}/full_data-*.xlsx` (anonymous authors, tag corrections, column cleanup — name is historical) | `weekly_data/{monday_str}/added_facebook_names_{monday_str}.xlsx` |
| `weekly_processing/google_sheet_editing.py` | `added_facebook_names_*.xlsx`, `ownership_database_*.csv`, filtering instructions Google Sheet | New `For_report_{monday_str}` Google Sheet |
| `weekly_processing/autovalidation.py` | most recent week in `spreadsheet_links.csv`, `vehicle_ids.csv`, OpenAI API | LLM-generated `validation_auto`, `confidence`, `reasoning`, `is_malfunction_auto`, `model_comparison_auto` columns written to each vehicle tab |

### Ad-hoc / standalone scripts

Run from the repo root:
- `ad_hoc/excel_upload.py` — builds upload-ready Excel from a completed tagging sheet; edit `sheet_id` and `vehicles_to_upload` at the top before running
- `ad_hoc/get_comp_set.py` — queries the DB for a competitor set and generates HTML report text; edit `competitor_set`, `output_name`, and date range at the top before running
- `ad_hoc/collect_report_sheets.py` — gathers every Google Sheet whose name starts with "For report"/"For_report" into one Drive folder (shortcuts by default; set `move_files = True` to move the originals). `google_sheet_editing.py` also calls its `add_new_sheet()` so each new weekly sheet is added automatically
- `ad_hoc/slack_listener.py` (WIP) — listens for Ford data files on Slack and triggers the pipeline
- `weekly_processing/tagging_accuracy.py` — weekly tagging-accuracy report, meant to run Monday mornings for the previous week (`--week YYYY-MM-DD` to pick a week, `--no-slack` to skip posting). Joins the week's sheet to `added_facebook_names_{week}.xlsx` on `id-3` and reports review volume, hit rate, how often analysts kept the LLM's tags on Hits (per vehicle and per LLM subcategory), autovalidation precision/recall and data-quality flags. Writes `accuracy_reports/tagging_accuracy_{week}.xlsx`, appends to `accuracy_reports/accuracy_history.csv`, and posts a summary plus the Excel to Slack channel `C0C7X7HG7SP` with `SLACK_BOT_TOKEN`. Reads the sheet as the service account (`get_service_account_creds()`), so `google_sheet_editing.py` shares each new sheet with it. Hit rate only counts tabs that mark Misses — before w/c 2026-10-12 some analysts marked only Hits

## Architecture Notes

- `auth.py` — shared OAuth logic: `get_creds()`, `SCOPES`, `TOKEN_PATH`, `CLIENT_SECRET_PATH`. Import from here in any script that needs Google auth.
- `helper_functions.py` — shared utilities: `get_monday_str()`, `google_sheet_to_dataframe()`, `get_cell_list()`, `get_spreadsheet_url()`, `save_spreadsheet_url()`
- `DataProcessor.py` — `DataProcessor` class for computing volume/sentiment/market stats per vehicle; `VEHICLE_MODELS` dict maps `desired_vehicle_id` → `(long_name, short_name)`. Used by `ad_hoc/get_comp_set.py`.
- `google_sheet_processor.py` — `GoogleSheetProcessor` class wrapping the Sheets and Drive APIs for a single spreadsheet. Key methods: `from_template()` (classmethod, copies a template), `read_tab()`, `clear_tab()`, `write_dataframe()`, `overwrite_tab()`, `hide_columns_except()`, `batch_update()`. Tab IDs are lazy-loaded and cached in `tab_ids`. Also exports `extract_file_id()` for parsing Google Drive URLs.
- Each pipeline script exposes a `main()` function; `run_all.py` imports and calls them directly (no subprocesses). `google_sheet_editing.main()` returns the new `spreadsheet_id`, which is passed directly to `autovalidation.main(spreadsheet_id)` — no file-based IPC needed.
- Filtering logic lives in `google_sheet_editing.py`: five boolean masks (owner authors, brand+model+country, all-domain, location-domain+country, thread title) are OR-combined per vehicle
- EU5 = DE, FR, ES, IT, UK; EU7 = DE, FR, ES, IT, UK, NL, NO (adds Netherlands and Norway) — check `is_EU7` flag per vehicle in the filtering instructions sheet
- Filtering instructions are fetched at runtime from a Google Sheet (tab: `ford_filtering_steps`, sheet ID `17kK-tOIpwBsYT_I98Me8SwGqq-5MBJZAXWxB7AlVsvQ`), indexed by `vehicle_model`
- `VISIBLE_COLS` in `google_sheet_editing.py` controls which columns are shown vs hidden in the output sheet
- `CODE_QUALITY_REVIEW.md` documents known bugs and bad practices — consult it before making changes

## Dependencies

`requirements.txt` only lists `slack-bolt`, `slack-sdk`, and `requests`. The remainder (pandas, openpyxl, google-api-python-client, google-auth-oauthlib, sqlalchemy, psycopg2, python-dotenv) must be installed separately.
