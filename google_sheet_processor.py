import re
import pandas as pd
from googleapiclient.discovery import build


def extract_file_id(url_or_id: str) -> str:
    """Accept a full Google Drive URL or a bare file ID and return the ID."""
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id


def _a1_col_to_index(col: str) -> int:
    """Convert A1 column letters to 0-based index (A->0, Z->25, AA->26, ...)."""
    col = col.strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
        else:
            break
    return n - 1


class GoogleSheetProcessor:
    """Wraps Google Sheets and Drive API operations for a single spreadsheet.

    Usage — open an existing sheet:
        processor = GoogleSheetProcessor(spreadsheet_id, creds)

    Usage — create a new sheet by copying a template:
        processor = GoogleSheetProcessor.from_template(template_url_or_id, "Sheet name", creds)
    """

    def __init__(self, spreadsheet_id: str, creds):
        self.spreadsheet_id = spreadsheet_id
        self._sheets = build("sheets", "v4", credentials=creds)
        self._drive = build("drive", "v3", credentials=creds)
        self._tab_ids: dict[str, int] | None = None

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_template(cls, template_url_or_id: str, name: str, creds) -> "GoogleSheetProcessor":
        """Copy a template spreadsheet and return a processor for the new file."""
        drive = build("drive", "v3", credentials=creds)
        file_id = extract_file_id(template_url_or_id)
        new_file = drive.files().copy(fileId=file_id, body={"name": name}).execute()
        return cls(new_file["id"], creds)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def url(self) -> str:
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"

    @property
    def tab_ids(self) -> dict[str, int]:
        """Mapping of tab name -> sheetId (lazy-loaded, cached)."""
        if self._tab_ids is None:
            meta = self._sheets.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            self._tab_ids = {
                s["properties"]["title"]: s["properties"]["sheetId"]
                for s in meta.get("sheets", [])
            }
        return self._tab_ids

    # ------------------------------------------------------------------
    # Read / write
    # ------------------------------------------------------------------

    def read_tab(self, tab_name: str) -> pd.DataFrame:
        """Read a tab and return it as a DataFrame."""
        result = self._sheets.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'"
        ).execute()
        rows = result.get("values", [])
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows[1:], columns=rows[0])

    def clear_tab(self, tab_name: str) -> None:
        self._sheets.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'!A:ZZ"
        ).execute()

    def write_dataframe(self, tab_name: str, df: pd.DataFrame) -> None:
        """Write a DataFrame to a tab, starting at A1 (header + data)."""
        values = [list(df.columns)] + df.astype(object).where(pd.notnull(df), "").values.tolist()
        self._sheets.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

    def overwrite_tab(self, tab_name: str, df: pd.DataFrame) -> None:
        """Clear a tab then write a DataFrame to it."""
        self.clear_tab(tab_name)
        self.write_dataframe(tab_name, df)

    # ------------------------------------------------------------------
    # Column visibility
    # ------------------------------------------------------------------

    def hide_columns_except(self, tab_name: str, visible_cols: list[str], max_cols: int | None = None) -> None:
        """Hide all columns in a tab except those listed in visible_cols (A1 notation, e.g. ["A","D","Z"]).

        max_cols overrides the auto-detected column limit when you know the sheet
        has more columns than the furthest visible column.
        """
        keep_idx = [_a1_col_to_index(c) for c in visible_cols]
        tab_id = self.tab_ids[tab_name]
        _max = max_cols if max_cols is not None else max(keep_idx) + 1
        self._apply_column_visibility(tab_id, keep_idx, _max)

    def _apply_column_visibility(self, tab_id: int, keep_cols: list[int], max_cols: int) -> None:
        if not keep_cols:
            return

        keep_sorted = sorted(set(keep_cols))
        end_index = max(keep_sorted[-1] + 1, max_cols)
        requests = []

        # Hide ranges between the columns we want to keep
        prev = -1
        for idx in keep_sorted:
            if idx - prev > 1:
                requests.append(_hide_range_request(tab_id, prev + 1, idx))
            prev = idx

        # Hide columns after the last visible column
        if prev + 1 < end_index:
            requests.append(_hide_range_request(tab_id, prev + 1, end_index))

        # Explicitly unhide the columns we want visible (in case they were hidden before)
        for idx in keep_sorted:
            requests.append(_unhide_range_request(tab_id, idx, idx + 1))

        self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests}
        ).execute()

    # ------------------------------------------------------------------
    # Row height
    # ------------------------------------------------------------------

    def reset_row_heights(self, tab_name: str, pixel_height: int = 21) -> None:
        """Set all rows in a tab to a fixed pixel height (default 21, Google's normal)."""
        tab_id = self.tab_ids[tab_name]
        self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": [{
                "updateDimensionProperties": {
                    "range": {"sheetId": tab_id, "dimension": "ROWS"},
                    "properties": {"pixelSize": pixel_height},
                    "fields": "pixelSize",
                }
            }]}
        ).execute()

    # ------------------------------------------------------------------
    # Low-level escape hatch
    # ------------------------------------------------------------------

    def batch_update(self, requests: list[dict]) -> dict:
        """Send arbitrary batchUpdate requests to the Sheets API."""
        return self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests}
        ).execute()


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _hide_range_request(tab_id: int, start: int, end: int) -> dict:
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": tab_id, "dimension": "COLUMNS", "startIndex": start, "endIndex": end},
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser",
        }
    }


def _unhide_range_request(tab_id: int, start: int, end: int) -> dict:
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": tab_id, "dimension": "COLUMNS", "startIndex": start, "endIndex": end},
            "properties": {"hiddenByUser": False},
            "fields": "hiddenByUser",
        }
    }
