import datetime
import pandas as pd
from urllib.parse import quote

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
        return ""
    temp = cell.strip().split(",")
    return [s.strip() for s in temp]

