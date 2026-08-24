import re
import time
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def _execute(request, max_retries: int = 6):
    """Execute an API request, retrying on 429 rate-limit errors with exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 429 and attempt < max_retries:
                wait = 2 ** attempt  # 1 s, 2 s, 4 s, 8 s, 16 s, 32 s
                time.sleep(wait)
            else:
                raise


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


def _col_index_to_a1(idx: int) -> str:
    """Convert 0-based column index to A1 column letters (0->A, 25->Z, 26->AA, ...)."""
    result = ""
    n = idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


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
        new_file = _execute(drive.files().copy(fileId=file_id, body={"name": name}))
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
            meta = _execute(self._sheets.spreadsheets().get(spreadsheetId=self.spreadsheet_id))
            self._tab_ids = {}
            self._frozen_rows: dict[str, int] = {}
            for s in meta.get("sheets", []):
                title = s["properties"]["title"]
                self._tab_ids[title] = s["properties"]["sheetId"]
                self._frozen_rows[title] = s["properties"].get("gridProperties", {}).get("frozenRowCount", 0)
        return self._tab_ids

    # ------------------------------------------------------------------
    # Read / write
    # ------------------------------------------------------------------

    def read_tab(self, tab_name: str) -> pd.DataFrame:
        """Read a tab and return it as a DataFrame."""
        result = _execute(self._sheets.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'"
        ))
        rows = result.get("values", [])
        if not rows:
            return pd.DataFrame()
        max_cols = max(len(r) for r in rows)
        header = rows[0] + [""] * (max_cols - len(rows[0]))
        data = [r + [""] * (max_cols - len(r)) for r in rows[1:]]
        return pd.DataFrame(data, columns=header)

    def clear_tab(self, tab_name: str) -> None:
        _execute(self._sheets.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'!A:ZZ"
        ))

    def _ensure_tab_size(self, tab_name: str, row_count: int, col_count: int) -> None:
        """Expand a tab's grid so it has at least row_count rows and col_count columns."""
        tab_id = self.tab_ids[tab_name]
        frozen = getattr(self, "_frozen_rows", {}).get(tab_name, 0)
        row_count = max(row_count, frozen + 1)
        _execute(self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": [{"updateSheetProperties": {
                "properties": {
                    "sheetId": tab_id,
                    "gridProperties": {"rowCount": row_count, "columnCount": col_count},
                },
                "fields": "gridProperties.rowCount,gridProperties.columnCount",
            }}]},
        ))

    def write_dataframe(self, tab_name: str, df: pd.DataFrame, chunk_size: int = 500) -> None:
        """Write a DataFrame to a tab, starting at A1 (header + data), in chunks."""
        df = df.copy()
        for col in df.select_dtypes(include=["datetimetz", "datetime64"]).columns:
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        rows = [list(df.columns)] + df.astype(object).where(pd.notnull(df), "").values.tolist()
        needed_rows = max(len(rows), 1)
        needed_cols = max(len(df.columns), 1)
        self._ensure_tab_size(tab_name, needed_rows, needed_cols)
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            start_row = i + 1  # 1-based
            _execute(self._sheets.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{tab_name}'!A{start_row}",
                valueInputOption="RAW",
                body={"values": chunk},
            ))

    def overwrite_tab(self, tab_name: str, df: pd.DataFrame) -> None:
        """Clear a tab then write a DataFrame to it."""
        self.clear_tab(tab_name)
        self.write_dataframe(tab_name, df)

    def fill_missing_translations(
        self,
        tab_name: str,
        source_col: str = "post_text",
        target_col: str = "translated_text",
        chunk_size: int = 500,
        poll_interval: float = 10.0,
        poll_timeout: float = 300.0,
    ) -> int:
        """Fill empty target_col cells with translations of source_col, written as static values.

        Writes GOOGLETRANSLATE formulas, polls until Sheets computes them, then overwrites
        each cell with its plain-text result so the sheet contains no live formulas.
        Returns the number of cells filled.
        """
        result = _execute(self._sheets.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'"
        ))
        rows = result.get("values", [])
        if not rows:
            return 0

        headers = rows[0]
        if source_col not in headers or target_col not in headers:
            return 0

        src_idx = headers.index(source_col)
        tgt_idx = headers.index(target_col)
        src_letter = _col_index_to_a1(src_idx)
        tgt_letter = _col_index_to_a1(tgt_idx)

        # Identify rows where target is empty but source has content (sheet row numbers, 1-based)
        formula_rows: list[int] = []
        for i, row in enumerate(rows[1:], start=2):
            tgt_val = row[tgt_idx] if tgt_idx < len(row) else ""
            src_val = row[src_idx] if src_idx < len(row) else ""
            if not str(tgt_val).strip() and str(src_val).strip():
                formula_rows.append(i)

        if not formula_rows:
            return 0

        # Write GOOGLETRANSLATE formulas
        formula_data = [
            {
                "range": f"'{tab_name}'!{tgt_letter}{r}",
                "values": [[f'=GOOGLETRANSLATE({src_letter}{r},"auto","en")']],
            }
            for r in formula_rows
        ]
        for i in range(0, len(formula_data), chunk_size):
            _execute(self._sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": formula_data[i:i + chunk_size]},
            ))

        # Poll until all formulas have computed (non-empty, non-formula value in each cell)
        formula_row_set = set(formula_rows)
        deadline = time.time() + poll_timeout
        col_value_map: dict[int, str] = {}
        while time.time() < deadline:
            time.sleep(poll_interval)
            col_result = _execute(self._sheets.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{tab_name}'!{tgt_letter}:{tgt_letter}",
                valueRenderOption="FORMATTED_VALUE",
            ))
            col_values = col_result.get("values", [])
            col_value_map = {i + 1: (row[0] if row else "") for i, row in enumerate(col_values)}
            pending = [
                r for r in formula_rows
                if not str(col_value_map.get(r, "")).strip()
            ]
            if not pending:
                break

        # Write computed values back as static text (RAW so they are not re-interpreted)
        static_data = [
            {
                "range": f"'{tab_name}'!{tgt_letter}{r}",
                "values": [[col_value_map.get(r, "")]],
            }
            for r in formula_rows
            if str(col_value_map.get(r, "")).strip()
        ]
        for i in range(0, len(static_data), chunk_size):
            _execute(self._sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"valueInputOption": "RAW", "data": static_data[i:i + chunk_size]},
            ))

        return len(formula_rows)

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

        _execute(self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests}
        ))

    # ------------------------------------------------------------------
    # Row height
    # ------------------------------------------------------------------

    def reset_row_heights(self, tab_name: str, pixel_height: int = 21) -> None:
        """Set all rows in a tab to a fixed pixel height (default 21, Google's normal)."""
        tab_id = self.tab_ids[tab_name]
        _execute(self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": [{
                "updateDimensionProperties": {
                    "range": {"sheetId": tab_id, "dimension": "ROWS"},
                    "properties": {"pixelSize": pixel_height},
                    "fields": "pixelSize",
                }
            }]}
        ))

    # ------------------------------------------------------------------
    # Low-level escape hatch
    # ------------------------------------------------------------------

    def batch_update(self, requests: list[dict]) -> dict:
        """Send arbitrary batchUpdate requests to the Sheets API."""
        return _execute(self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests}
        ))


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
