import datetime
from helper_functions import get_monday_str, google_sheet_to_dataframe, get_cell_list
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import re
from googleapiclient.errors import HttpError
import os
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pandas.api.types import is_object_dtype
from google.auth.exceptions import RefreshError

load_dotenv()

EU7 = ["DE", "FR", "ES", "IT", "UK"]
EU5 = ["DE", "FR", "ES", "IT", "UK", "NL", "NO"] 

def a1_col_to_index(col: str) -> int:
    """Convert A1 column letters to 0-based index (A->0, Z->25, AA->26, ...)."""
    col = col.strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
        else:
            break
    return n - 1  # 0-based

def hide_all_but_columns(sheets_service, spreadsheet_id: str, tab_id: int, keep_cols: list[int], max_cols: int):
    """Hide all columns up to max_cols, then unhide the specified keep_cols."""
    if not keep_cols:
        return
    end_index = max(max(keep_cols) + 1, max_cols)

    requests = []
    # Hide everything in [0, end_index)
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": tab_id,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex": end_index,
            },
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser",
        }
    })
    # Unhide the columns to keep
    for idx in sorted(set(keep_cols)):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": tab_id,
                    "dimension": "COLUMNS",
                    "startIndex": idx,
                    "endIndex": idx + 1,
                },
                "properties": {"hiddenByUser": False},
                "fields": "hiddenByUser",
            }
        })

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()


# accept a URL or an ID for the template file id
def extract_file_id(url_or_id: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SCOPES = [
    "https://www.googleapis.com/auth/drive", 
    "https://www.googleapis.com/auth/spreadsheets"
]

filtering_instructions = google_sheet_to_dataframe("ford_filtering_steps", "17kK-tOIpwBsYT_I98Me8SwGqq-5MBJZAXWxB7AlVsvQ")
filtering_instructions.set_index("vehicle_model", inplace=True)

TOKEN_PATH = os.getenv("GOOGLE_OAUTH_TOKEN_JSON")
CLIENT_SECRET_PATH = os.getenv("GOOGLE_OAUTH_CLIENT_JSON")   # OAuth client (Desktop app), not a service account

def get_creds():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            return creds
        except RefreshError:
            # refresh token revoked/invalid – fall through to new login
            pass

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
    with open(TOKEN_PATH, "w") as f:
        f.write(creds.to_json())
    return creds

creds = get_creds()
drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)

monday_str = get_monday_str()

# For each vehicle id in current ownership database, add posts from vehicle's owners into corresponding tab
weekly_data = pd.read_excel(f"weekly_data/{monday_str}/added_facebook_names_{monday_str}.xlsx")
ownership_database = pd.read_csv(f"ownership_databases/ownership_database_{monday_str}.csv")

# Get ownership counts for each author
ownership_counts = (
    ownership_database
    .loc[ownership_database["ownership_status"] == "Owner", ["author_name", "desired_vehicle_id"]]
    .groupby("author_name")["desired_vehicle_id"]
    .nunique()
    .astype(int)
)

def add_ownership_count_column(df: pd.DataFrame) -> pd.DataFrame: 
    df["ownership_count"] = df["author_name"].map(ownership_counts).fillna(0).astype(int)
    return df

TEMPLATE_FILE_ID = extract_file_id("https://docs.google.com/spreadsheets/d/1WridMYMZ4uuJWrNLmkG6JSTjLk7oEUi_ZSaDKvA2AfQ/edit#gid=0")

# Copy the template for this week
copied_file = {"name": f"For report_{monday_str}"}
new_file = drive_service.files().copy(fileId=TEMPLATE_FILE_ID, body=copied_file).execute()
SPREADSHEET_ID = new_file["id"]

# Columns to remain visible in vehicle tabs on google sheet
VISIBLE_COLS = ["A","D","F","K","L","M","O","Z","AE","AF","AG","AH","AV","BB","BC","BI","BJ"]

# Create new spreadsheet
meta = sheets_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
existing_tabs = {s["properties"]["title"] for s in meta.get("sheets", [])}
tab_ids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}

# grouped is a mini dataframe of authors for each (vehicle_id, vehicle_model)
STATUS_ORDER = {"Owner": 3, "Pre-Ownership": 2, "Showing Interest": 1}
grouped = ownership_database.groupby(["desired_vehicle_id", "vehicle_name"], dropna=False)

# Iterating through ownership database and adding all posts from vehicle owners to corresponding vehicle tab
# Note that for this code to work, the vehicle name in ownership table must be the same as the tab name
for (veh_id, vehicle_name), grp in grouped:
    if pd.isna(vehicle_name) or str(vehicle_name).strip() == "":
        continue  # skip rows without a usable tab name
    
    tab_id = tab_ids[vehicle_name]
    # Resolve each author's *best* status by precedence
    # two underscores before rank to signify that its a throwaway column
    grp = grp.copy()
    grp["__rank"] = grp["ownership_status"].map(STATUS_ORDER).fillna(0)
    best = (
        grp.sort_values("__rank", ascending=False)
           .drop_duplicates(subset=["author_name"], keep="first")
    )
    author_to_status = dict(zip(best["author_name"], best["ownership_status"]))
    authors = set(author_to_status.keys())
    
    brands = get_cell_list(filtering_instructions, vehicle_name, "brand")
    model_searches = get_cell_list(filtering_instructions, vehicle_name, "model_searches")
    location_domains = get_cell_list(filtering_instructions, vehicle_name, "location_domain_posts")
    all_domains = get_cell_list(filtering_instructions, vehicle_name, "all_domain_posts")
    thread_searches = get_cell_list(filtering_instructions, vehicle_name, "thread_title")
    is_eu7 = filtering_instructions.loc[vehicle_name]["is_EU7"]
    country_codes = EU7 if is_eu7 else EU5

    # Build filtering masks
    m_authors = weekly_data["author_name"].isin(authors)
    m_models = (
        weekly_data["brand"].isin(brands) & 
        weekly_data["model"].isin(model_searches) & 
        weekly_data["country_code"].isin(country_codes)
    ) if brands and model_searches else False
    
    m_all_domains = weekly_data["domain"].isin(all_domains)
    m_loc_domains = (
        weekly_data["domain"].isin(location_domains) & 
        weekly_data["country_code"].isin(country_codes)
    ) if location_domains else False
    m_threads = weekly_data["thread_title"].isin(thread_searches)
    
    include_mask = m_authors | m_models | m_all_domains | m_loc_domains | m_threads
    data_to_add = weekly_data.loc[include_mask].copy()
    
    # Add vehicle_ownership_status column
    data_to_add["vehicle_ownership_status"] = pd.where(
        data_to_add["author_name"].isin(authors),
        data_to_add["author_name"].map(author_to_status)
    )
    
    # Add count of owned vehicles based on ownership_database
    data_to_add["ownership_count"] = data_to_add["author_name"].map(ownership_counts).fillna(0).astype(int)
    cols = [c for c in data_to_add.columns]
    
    # Clear the tab
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=f"'{vehicle_name}'!A:ZZ"
    ).execute()

    # Prepare values: header + data; convert NaNs to "" for Sheets
    values = [cols] + data_to_add.astype(object).where(pd.notnull(data_to_add), "").values.tolist()

    # Write starting at A1
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{vehicle_name}'!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()
    
    keep_idx = [a1_col_to_index(c) for c in VISIBLE_COLS]
    # Use the greater of your data width vs the furthest column you plan to keep
    max_cols = max(len(cols), max(keep_idx) + 1)
    hide_all_but_columns(sheets_service, SPREADSHEET_ID, tab_id, keep_idx, max_cols)
    
    print(f"Pasted all posts into {vehicle_name} tab written by its verified owners!")

print(f"Populated: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")