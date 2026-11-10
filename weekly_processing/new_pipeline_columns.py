"""
Adds the trained-tagging pipeline's results to the week's sheet, as five columns right after each vehicle tab's
Validation column, so analysts can compare them with their own call as they review.

Columns: New autovalidation (Hit/Maybe/Miss for this post in this tab, "Not in tab" if the pipeline did not put it
there), New tags (every tag and sentiment it gave the post), New: this tag? (whether it also has the row's tag, and
with what sentiment), New ownership, New country.

The results are read from S3 (exports/ford/trained_tagging/ and exports/ford/improved_upstream/), written by the
platform's ford-trained-tagging pipeline the same Monday. Runs last in run_all.py, after autovalidation, because
autovalidation rewrites whole tabs. If this week's results are not there yet it waits (NEW_PIPELINE_WAIT_MINUTES,
default 90), then gives up without touching the sheet. Tabs that already have the columns are skipped, so it is safe
to rerun.

    python weekly_processing/new_pipeline_columns.py [--week YYYY-MM-DD] [--no-wait]
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import datetime
import io
import os
import time

import boto3
import pandas as pd
from dotenv import load_dotenv

from auth import get_creds
from google_sheet_processor import GoogleSheetProcessor, extract_file_id
from helper_functions import get_monday_str, get_spreadsheet_url

load_dotenv(PROJECT_ROOT / ".env")

BUCKET = "fluency-data-platform"
TAGS_PREFIX = "exports/ford/trained_tagging/"
UPSTREAM_PREFIX = "exports/ford/improved_upstream/"
HEADERS = ["New autovalidation", "New tags", "New: this tag?", "New ownership", "New country"]
WAIT_MINUTES = int(os.getenv("NEW_PIPELINE_WAIT_MINUTES", "90"))
POLL_MINUTES = 5
MIN_MATCH_SHARE = 0.5  # below this the results are probably for a different week: write nothing

# Older names of tags that may still appear in a sheet -> the names the pipeline uses
TAG_ALIASES = {
    "Overall Satisfaction With the Car": "Overall Satisfaction with the Vehicle",
    "Overall Satisfaction with the Car": "Overall Satisfaction with the Vehicle",
    "Range": "Range/Consumption",
    "Consumption/Range": "Range/Consumption",
    "Switching to Another Brand": "Switching to Another Vehicle",
    "Overall Disappointment With the Brand": "Overall Satisfaction with the Brand",
    "Communication From Brand": "Communication from Brand",
    "Aftersales (OEM App Support)": "OEM App Support Team",
    "Owners Manual": "Owner's Manual",
    "Communication With Dealer": "Communication with Dealer",
    "Infotainment": "Infotainment/Centerstack",
}


# ------------------------------------------------------------------
# Finding and reading the week's results
# ------------------------------------------------------------------

def _keys(s3, prefix: str, suffix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(suffix)]
    return keys


def _stem(key: str) -> str:
    """'exports/ford/trained_tagging/2026-11-09_163643_verdicts.parquet' -> '2026-11-09_163643'"""
    return "_".join(key.rsplit("/", 1)[-1].split("_")[:2])


def find_results(s3, monday_str: str) -> dict | None:
    """S3 keys of the newest complete run made in the week starting monday_str, or None if there is none yet."""
    end = (datetime.date.fromisoformat(monday_str) + datetime.timedelta(days=7)).isoformat()
    in_week = lambda stem: monday_str <= stem[:10] < end
    verdicts = {_stem(k): k for k in _keys(s3, TAGS_PREFIX, "_verdicts.parquet") if in_week(_stem(k))}
    feedback = {_stem(k): k for k in _keys(s3, TAGS_PREFIX, "_feedback.parquet")}
    complete = sorted(st for st in verdicts if st in feedback)  # feedback is written last
    if not complete:
        return None
    run = complete[-1]
    mentions = sorted((_stem(k), k) for k in _keys(s3, UPSTREAM_PREFIX, "_vehicle_mentions.parquet")
                      if in_week(_stem(k)) and _stem(k) <= run)
    return {"run": run, "verdicts": verdicts[run], "feedback": feedback[run],
            "mentions": mentions[-1][1] if mentions else None}


def _read_parquet(s3, key: str, columns: list[str]) -> pd.DataFrame:
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body), columns=columns)


def read_results(s3, keys: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    verdicts = _read_parquet(s3, keys["verdicts"], ["url", "tab", "validation_auto"])
    feedback = _read_parquet(s3, keys["feedback"], ["url", "ownership_status", "feedback_subcategory", "feedback_sentiment"])
    mentions = (_read_parquet(s3, keys["mentions"], ["url", "country_code"]) if keys["mentions"]
                else pd.DataFrame(columns=["url", "country_code"]))
    return verdicts, feedback, mentions


# ------------------------------------------------------------------
# Building the five values for each row of a tab
# ------------------------------------------------------------------

def _clean(x) -> str:
    return x.strip() if isinstance(x, str) and x.strip().lower() not in ("", "nan", "none") else ""


def build_lookup(verdicts: pd.DataFrame, feedback: pd.DataFrame, mentions: pd.DataFrame) -> dict:
    """verdict per (tab, url); tags, tag -> sentiment and ownership per url; country per url."""
    posts = {}
    for url, g in feedback.groupby("url"):
        pairs = sorted({(_clean(t), _clean(se) or "Neutral")
                        for t, se in zip(g.feedback_subcategory, g.feedback_sentiment) if _clean(t)})
        own = next((o for o in map(_clean, g.ownership_status) if o), "")
        posts[url] = {"tags": "; ".join(f"{t}: {se}" for t, se in pairs), "sentiment": dict(pairs), "own": own}
    country = {u: _clean(c) for u, c in zip(mentions.url, mentions.country_code) if _clean(c)}
    return {"verdict": {(t, u): v for t, u, v in zip(verdicts.tab, verdicts.url, verdicts.validation_auto)},
            "post": posts, "country": country, "tabs": set(verdicts.tab)}


def tab_values(lookup: dict, tab: str, urls: list[str], row_tags: list[str]) -> tuple[list[list[str]], int]:
    """The five new values for each row of a tab, and how many rows the pipeline put in that tab."""
    values, matched = [], 0
    for url, tag in zip(urls, row_tags):
        if not url:
            values.append([""] * len(HEADERS))
            continue
        verdict = lookup["verdict"].get((tab, url))
        post = lookup["post"].get(url)
        matched += verdict is not None
        if verdict is None and post is None:
            values.append(["Not in tab", "", "", "", ""])
            continue
        post = post or {"tags": "", "sentiment": {}, "own": ""}
        sentiment = post["sentiment"].get(TAG_ALIASES.get(tag, tag))
        this_tag = (f"Yes, {sentiment}" if sentiment else "No") if tag else ""
        values.append([verdict or "Not in tab", post["tags"], this_tag, post["own"], lookup["country"].get(url, "")])
    return values, matched


# ------------------------------------------------------------------
# Writing to the sheet
# ------------------------------------------------------------------

def insert_columns(sheet: GoogleSheetProcessor, tab: str, first: int, n_rows: int) -> None:
    """Insert the empty, visible, unformatted columns at 0-based index `first`. They copy the column to their right,
    not Validation's dropdown; any dropdown left on them (including a Table column type) is removed."""
    tab_id, last = sheet.tab_ids[tab], first + len(HEADERS)
    cols = {"sheetId": tab_id, "startColumnIndex": first, "endColumnIndex": last}
    sheet.batch_update([
        {"insertDimension": {"range": {"sheetId": tab_id, "dimension": "COLUMNS", "startIndex": first, "endIndex": last},
                             "inheritFromBefore": False}},
        {"setDataValidation": {"range": {**cols, "startRowIndex": 0, "endRowIndex": n_rows}}},
        {"repeatCell": {"range": {**cols, "startRowIndex": 0, "endRowIndex": n_rows}, "cell": {},
                        "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {**cols, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                        "fields": "userEnteredFormat.textFormat.bold"}},
        {"updateDimensionProperties": {"range": {"sheetId": tab_id, "dimension": "COLUMNS", "startIndex": first,
                                                 "endIndex": last},
                                       "properties": {"hiddenByUser": False}, "fields": "hiddenByUser"}},
    ])
    requests = []
    for t in sheet.tables(tab):
        start, end = t["range"].get("startColumnIndex", 0), t["range"]["endColumnIndex"]
        if not start <= first < end:
            continue
        props = {p["columnIndex"]: p for p in t.get("columnProperties", [])}
        for i, h in enumerate(HEADERS):
            if first + i < end:
                props[first + i - start] = {"columnIndex": first + i - start, "columnName": h, "columnType": "TEXT"}
        requests.append({"updateTable": {"table": {"tableId": t["tableId"], "columnProperties": list(props.values())},
                                         "fields": "columnProperties"}})
    if requests:
        sheet.batch_update(requests)


def add_to_tab(sheet: GoogleSheetProcessor, tab: str, header: list[str], values: list[list[str]]) -> None:
    first = header.index("Validation") + 1
    insert_columns(sheet, tab, first, len(values) + 1)
    try:
        sheet.write_values(tab, 0, first, [HEADERS] + values)
    except Exception:
        sheet.batch_update([{"deleteDimension": {"range": {"sheetId": sheet.tab_ids[tab], "dimension": "COLUMNS",
                                                           "startIndex": first, "endIndex": first + len(HEADERS)}}}])
        raise


# ------------------------------------------------------------------

def main(monday_str: str | None = None, wait: bool = True) -> None:
    monday_str = monday_str or get_monday_str()
    url = get_spreadsheet_url(monday_str)
    if url is None:
        print(f"No sheet for the week of {monday_str} in spreadsheet_links.csv — skipping new pipeline columns.")
        return

    s3 = boto3.client("s3")
    deadline = time.time() + (WAIT_MINUTES * 60 if wait else 0)
    while (keys := find_results(s3, monday_str)) is None and time.time() < deadline:
        print(f"  New pipeline results for {monday_str} not in S3 yet; checking again in {POLL_MINUTES} min.")
        time.sleep(POLL_MINUTES * 60)
    if keys is None:
        print(f"No new pipeline results for the week of {monday_str} — sheet left as it is. "
              f"Rerun later with: python weekly_processing/new_pipeline_columns.py --week {monday_str}")
        return
    print(f"Using new pipeline run {keys['run']}")
    lookup = build_lookup(*read_results(s3, keys))

    sheet = GoogleSheetProcessor(extract_file_id(url), get_creds())
    vehicle_tabs = pd.read_csv(PROJECT_ROOT / "vehicle_ids.csv")["vehicle_name"].tolist()
    planned, matched_all, rows_all = [], 0, 0
    for tab in vehicle_tabs:
        if tab not in sheet.tab_ids or tab not in lookup["tabs"]:
            continue
        df = sheet.read_tab(tab)
        if df.empty:
            continue
        header = list(df.columns)
        if HEADERS[0] in header:
            print(f"  {tab}: already has the new columns, skipped")
            continue
        if not {"Validation", "url", "feedback_subcategory"} <= set(header):
            print(f"  {tab}: no Validation/url/feedback_subcategory column, skipped")
            continue
        urls = df.iloc[:, header.index("url")].tolist()
        values, matched = tab_values(lookup, tab, urls, df.iloc[:, header.index("feedback_subcategory")].tolist())
        planned.append((tab, header, values, matched))
        matched_all += matched
        rows_all += sum(1 for u in urls if u)

    if rows_all and matched_all / rows_all < MIN_MATCH_SHARE:
        print(f"Only {matched_all:,} of {rows_all:,} rows match run {keys['run']} — probably a different week. "
              "Sheet left as it is.")
        return

    done = failed = 0
    for tab, header, values, matched in planned:
        try:
            add_to_tab(sheet, tab, header, values)
            done += 1
            print(f"  {tab}: {matched} of {len(values)} rows matched")
        except Exception as e:
            failed += 1
            print(f"  {tab}: FAILED, new columns removed: {e}")
    print(f"New pipeline columns: {done} tabs filled, {failed} failed ({matched_all:,} of {rows_all:,} rows matched).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--week", help="Monday of the sheet's week, YYYY-MM-DD (default: this week)")
    parser.add_argument("--no-wait", action="store_true", help="don't wait for the results if they are not in S3 yet")
    args = parser.parse_args()
    main(args.week, wait=not args.no_wait)
