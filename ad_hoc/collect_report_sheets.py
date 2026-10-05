import sys
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from googleapiclient.discovery import build
from auth import get_creds
from google_sheet_processor import _execute

# Name of the Drive folder to create (reused if it already exists in My Drive root)
folder_name = "For report sheets"

# False = add shortcuts to the folder (originals stay where they are)
# True  = move the original sheets into the folder
move_files = False

# Matches "For report ..." and "For_report_..." (case insensitive)
NAME_PATTERN = re.compile(r"^for[ _]report", re.IGNORECASE)

FOLDER_MIME = "application/vnd.google-apps.folder"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"


def list_all(drive, query: str, fields: str) -> list[dict]:
    files, page_token = [], None
    while True:
        resp = _execute(drive.files().list(
            q=query,
            fields=f"nextPageToken, files({fields})",
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ))
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            return files


def get_or_create_folder(drive, name: str) -> str:
    escaped = name.replace("'", "\\'")
    existing = list_all(
        drive,
        f"name = '{escaped}' and mimeType = '{FOLDER_MIME}' and 'root' in parents and trashed = false",
        "id, name",
    )
    if existing:
        print(f"Using existing folder '{name}'")
        return existing[0]["id"]
    folder = _execute(drive.files().create(body={"name": name, "mimeType": FOLDER_MIME}, fields="id"))
    print(f"Created folder '{name}'")
    return folder["id"]


def add_sheet_to_folder(drive, sheet: dict, folder_id: str) -> None:
    """Shortcut (or move, if move_files) a sheet into the folder. `sheet` needs id, name, parents."""
    if move_files:
        _execute(drive.files().update(
            fileId=sheet["id"],
            addParents=folder_id,
            removeParents=",".join(sheet.get("parents", [])),
            fields="id",
            supportsAllDrives=True,
        ))
        print(f"  moved: {sheet['name']}")
    else:
        _execute(drive.files().create(
            body={
                "name": sheet["name"],
                "mimeType": SHORTCUT_MIME,
                "parents": [folder_id],
                "shortcutDetails": {"targetId": sheet["id"]},
            },
            fields="id",
            supportsAllDrives=True,
        ))
        print(f"  shortcut added: {sheet['name']}")


def add_new_sheet(sheet_id: str, creds) -> None:
    """Add a single newly created sheet to the report folder (used by the weekly pipeline)."""
    drive = build("drive", "v3", credentials=creds)
    folder_id = get_or_create_folder(drive, folder_name)
    sheet = _execute(drive.files().get(fileId=sheet_id, fields="id, name, parents", supportsAllDrives=True))
    add_sheet_to_folder(drive, sheet, folder_id)


def main():
    drive = build("drive", "v3", credentials=get_creds())
    folder_id = get_or_create_folder(drive, folder_name)

    # Drive's 'contains' is a word-prefix match, so narrow further with the regex
    candidates = list_all(
        drive,
        f"name contains 'For' and mimeType = '{SHEET_MIME}' and trashed = false",
        "id, name, parents",
    )
    sheets = sorted((f for f in candidates if NAME_PATTERN.match(f["name"])), key=lambda f: f["name"])
    print(f"Found {len(sheets)} matching sheets")

    # Skip sheets already in the folder (as the file itself or as a shortcut)
    in_folder = list_all(drive, f"'{folder_id}' in parents and trashed = false", "id, shortcutDetails")
    already_there = {f["id"] for f in in_folder} | {
        f["shortcutDetails"]["targetId"] for f in in_folder if "shortcutDetails" in f
    }

    for sheet in sheets:
        if sheet["id"] in already_there:
            print(f"  skip (already in folder): {sheet['name']}")
            continue
        add_sheet_to_folder(drive, sheet, folder_id)

    print(f"Done: https://drive.google.com/drive/folders/{folder_id}")


if __name__ == "__main__":
    main()
