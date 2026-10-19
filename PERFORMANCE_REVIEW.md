# Performance Review — Weekly Pipeline Bottlenecks

Written 2026-10-12, from reading the code plus w/c 2026-10-05 volumes (55,710 rows across 28 vehicle tabs, ~2,000 per tab, max ~3,700; input file 210k rows × 67 columns, 115 MB).

No full-pipeline timings exist yet, so the ranking below is an estimate. Do item 0 first to confirm it.

Line numbers are as of commit `7ecc2ec`.

---

## 0. Add per-step timing to `run_all.py`

**File:** `weekly_processing/run_all.py`

There are no logs or timings, so every estimate here is unconfirmed. Wrap each step in `time.perf_counter()` and print the elapsed time. Ideally, also log per-tab times inside `google_sheet_editing.py` and `autovalidation.py`.

- [ ] Done

---

## 1. `autovalidation.py` — likely the largest cost

### 1a. The `prompt` column is written back into the sheet

**File:** `weekly_processing/autovalidation.py:216`, `:247`

```python
df["prompt"] = df.iloc[:, 10].apply(self.generate_prompt)
...
sheet.write_dataframe(tab_name, df)   # df still contains "prompt"
```

Each prompt is about 4.7 KB, so this uploads roughly 260 MB of text a week. It makes the write slow and the sheet heavy for analysts to open. The column also probably isn't hidden: it's added after `hide_columns_except` ran, so it may be visible.

**Fix:** drop `prompt` before writing, and remove any existing `prompt` column from this week's sheet.

- [ ] Done

### 1b. The whole tab is rewritten instead of just the output columns

**File:** `weekly_processing/autovalidation.py:247`

Only the 5 `OUTPUT_COLS` change, but all ~70 columns are rewritten.

**Fix:** write only the output columns. Either use a `values().batchUpdate` per column range, or add a `write_columns()` helper to `GoogleSheetProcessor`.

- [ ] Done

### 1c. Tabs are processed one after another

**File:** `weekly_processing/autovalidation.py:236-247`

Each tab runs its own `asyncio.run()`. The next tab can't start until the slowest request in the current one finishes, and a single request can take up to 60s × 3 attempts. With 28 tabs, those waits add up.

**Fix:** read all tabs, build every request, and run them in one event loop with one semaphore. Then write the results back per tab. This also removes the "fresh processor per tab" workaround.

- [ ] Done

### 1d. A new HTTP session for every request

**File:** `weekly_processing/autovalidation.py:150`

`aiohttp.ClientSession` is created inside the per-request loop, so every one of the ~55k calls pays a new connection and TLS handshake.

**Fix:** create one session per run and share it across requests.

- [ ] Done

### 1e. Too few retries, so rate-limited rows come back blank

**File:** `weekly_processing/autovalidation.py:129`, `:164-169`

There are only 2 retries, 1s and 2s apart, and the `Retry-After` header is ignored. On a 429 the row ends up blank. Last week: 13 blank in Kuga MCA and 10 in Puma MCA. `max_concurrent_requests=1000` (line 209) makes 429s more likely.

**Fix:** retry more times with longer backoff and honour `Retry-After`. Set concurrency from the account's OpenAI rate limits, not a fixed 1000.

- [ ] Done

### 1f. Prompt layout prevents prefix caching

**File:** `weekly_processing/autovalidation.py:38-125`

`{text}` comes at the top of `PROMPT_TEMPLATE`, before about 1,200 tokens of fixed instructions. OpenAI's automatic prompt caching only applies to a shared prefix, so nothing gets cached. Volume is about 55k calls and 65M input tokens a week.

**Fix:** move the instructions into a system message, or move `{text}` to the end. Check that classifications don't change (compare against a past week).

- [ ] Done

### 1g. Re-reads tabs that were just written

**File:** `weekly_processing/autovalidation.py:238`

`google_sheet_editing.main()` already holds each tab's DataFrame in memory. Autovalidation reads them back from Sheets anyway. This is minor; consider it when restructuring 1c.

- [ ] Done

---

## 2. Translation polling runs tab by tab

**File:** `weekly_processing/google_sheet_editing.py:162-165`, `google_sheet_processor.py:182-274`

`fill_missing_translations` runs per tab. It writes `GOOGLETRANSLATE` formulas, then sleeps at least 10s and polls for up to 300s. Across 28 tabs that's at least about 5 minutes, and up to 2+ hours. On timeout, the untranslated rows are left blank and nothing is reported.

**Fix:** split the method into a write-formulas phase and a poll-and-freeze phase. Write the formulas for every tab first, then poll all tabs together, ideally with a single `values().batchGet`. Log any rows still blank after the timeout.

- [ ] Done

---

## 3. Sheets write-quota pressure in `google_sheet_editing.py`

**File:** `weekly_processing/google_sheet_editing.py:153-156`, `google_sheet_processor.py:12-29`

Each vehicle makes these calls one after another: `clear_tab` → `_ensure_tab_size` → about 4 chunked `values().update` calls of 500 rows → `copy_data_validation` → `hide_columns_except` → `reset_row_heights`. With translations included, that's about 300+ write calls a run. Google allows about 60 writes per minute per user, so this takes at least 5 minutes. On top of that, `_execute` backs off for up to 32s on each 429.

**Fix:**
- Combine resize, dropdown copy, column hiding and row heights into one `batchUpdate` per tab. Or build the requests for all tabs and send them in a few calls.
- Send the data with one `values().batchUpdate` per tab (or across tabs) instead of separate 500-row updates, staying under the payload limit.
- `clear_tab` can be folded into the same batch, or skipped where the template tab is already empty.

- [ ] Done

---

## 4. Excel as the intermediate format (~2.5 min)

**File:** `weekly_processing/add_facebook_names.py:19`, `:65`; `weekly_processing/google_sheet_editing.py:67`

Timed on a 20k-row sample and extrapolated to the full 210k rows:

| Step | Time |
|---|---|
| Read `full_data` | ~0.6 min |
| Write `added_facebook_names_*.xlsx` | ~1.2 min |
| Read it again | ~0.6 min |

**Fix:** write the intermediate file as parquet (or pickle) and read that in `google_sheet_editing.py`. Keep the xlsx only if someone opens it by hand. `tagging_accuracy.py` also reads `added_facebook_names_{week}.xlsx` and would need updating. Alternatively, have `run_all.py` pass the DataFrame in memory.

- [ ] Done

---

## 5. Ownership database query — not measured

**File:** `weekly_processing/get_ownership_database.py:27-38`

The query runs `DISTINCT` + `ORDER BY` over four joined tables, from `'2025-01-01'` to `CURRENT_DATE`. The window grows every week, and the whole result is rebuilt from scratch each time. It wasn't timed (no database access during the review).

**Fix (if item 0 shows it's slow):** fetch only posts since the last cached `ownership_database_*.csv` and add them to it. Drop `ORDER BY`, which nothing downstream relies on. Check the database for indexes on `post_date`, `desired_vehicle_id` and `hit`.

- [ ] Done

---

## Manual steps (not code speed, but block unattended runs)

- `full_data-*.xlsx` has to be downloaded to `~/Downloads` by hand. `ad_hoc/slack_listener.py` (WIP) is the intended fix.
- The OAuth token sometimes needs browser consent again, which blocks a headless run. Consider the service account for the steps that can use it.

---

## Suggested order

1. Item 0 (timing)
2. 1a and 1b (stop uploading prompts and full tabs; quick wins)
3. 1c and 1d (one event loop, one session)
4. Item 2 (batched translation polling)
5. Item 3 (batched formatting requests)
6. 1e, 1f, then items 4 and 5
