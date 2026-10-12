import datetime
from pathlib import Path
import pandas as pd
from urllib.parse import quote

# One row per week of data: the Monday it belongs to and the URL of that week's Google Sheet
SPREADSHEET_LINKS_PATH = Path(__file__).resolve().parent / "spreadsheet_links.csv"

def get_monday_str() -> str: 
    today = datetime.date.today()
    monday = today - datetime.timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")

def google_sheet_to_dataframe(tab_name: str, sheet_id: str) -> pd.DataFrame: 
    tab_name_encoded = quote(tab_name)
    sheet_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?"
        f"tqx=out:csv&sheet={tab_name_encoded}"
    )
    return pd.read_csv(sheet_url, encoding="utf-8")

def get_cell_list(df: pd.DataFrame, index: str, column: str) -> list: 
    cell = str(df.loc[index][column])
    if cell == "": 
        return []  # return empty list so downstream .isin() calls don't get a stray string
    temp = cell.strip().split(",")
    return [s.strip() for s in temp]

def get_spreadsheet_url(monday_str: str | None = None) -> str | None:
    """URL of the Google Sheet for the week starting monday_str, or the most recent week's if None."""
    if not SPREADSHEET_LINKS_PATH.exists():
        return None
    links = pd.read_csv(SPREADSHEET_LINKS_PATH, dtype=str)
    if monday_str is not None:
        links = links[links["week"] == monday_str]
    if links.empty:
        return None
    return links.sort_values("week")["url"].iloc[-1]

def save_spreadsheet_url(monday_str: str, url: str) -> None:
    """Record the Google Sheet for the week starting monday_str, replacing any earlier sheet for that week."""
    links = pd.DataFrame([{
        "week": monday_str,
        "url": url,
        "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }])
    if SPREADSHEET_LINKS_PATH.exists():
        old_links = pd.read_csv(SPREADSHEET_LINKS_PATH, dtype=str)
        links = pd.concat([old_links[old_links["week"] != monday_str], links], ignore_index=True)
    links.sort_values("week").to_csv(SPREADSHEET_LINKS_PATH, index=False)
