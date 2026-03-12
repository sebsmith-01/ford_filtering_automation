import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from helper_functions import get_monday_str, google_sheet_to_dataframe, get_cell_list
from google_sheet_processor import GoogleSheetProcessor, _a1_col_to_index
import pandas as pd
import os
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import numpy as np

load_dotenv()

EU5 = ["DE", "FR", "ES", "IT", "UK"]
EU7 = ["DE", "FR", "ES", "IT", "UK", "NL", "NO"]

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

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

monday_str = get_monday_str()

filtering_instructions = google_sheet_to_dataframe("ford_filtering_steps", "17kK-tOIpwBsYT_I98Me8SwGqq-5MBJZAXWxB7AlVsvQ")
filtering_instructions.set_index("vehicle_model", inplace=True)

# For each vehicle id in current ownership database, add posts from vehicle's owners into corresponding tab
weekly_data = pd.read_excel(PROJECT_ROOT / f"weekly_data/{monday_str}/added_facebook_names_{monday_str}.xlsx")
ownership_database = pd.read_csv(PROJECT_ROOT / f"ownership_databases/ownership_database_{monday_str}.csv")

ownership_counts = (
    ownership_database
    .loc[ownership_database["ownership_status"] == "Owner", ["author_name", "desired_vehicle_id"]]
    .groupby("author_name")["desired_vehicle_id"]
    .nunique()
    .astype(int)
)

TEMPLATE_URL = "https://docs.google.com/spreadsheets/d/1WridMYMZ4uuJWrNLmkG6JSTjLk7oEUi_ZSaDKvA2AfQ/edit#gid=0"

sheet = GoogleSheetProcessor.from_template(TEMPLATE_URL, f"For report_{monday_str}", creds)

VISIBLE_COLS = ["A","D","F","K","L","O","Z","AE","AI","AJ","AY","BC","BD","BH","BN","BO","BP"]

STATUS_ORDER = {"Owner": 3, "Pre-Ownership": 2, "Showing Interest": 1}
grouped = ownership_database.groupby(["desired_vehicle_id", "vehicle_name"], dropna=False)

# Iterating through ownership database and adding all posts from vehicle owners to corresponding vehicle tab
# Note that for this code to work, the vehicle name in ownership table must be the same as the tab name
for (veh_id, vehicle_name), grp in grouped:
    if pd.isna(vehicle_name) or str(vehicle_name).strip() == "":
        print(f"skipping {vehicle_name}")
        continue  # skip rows without a usable tab name

    # Resolve each author's *best* status by precedence
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

    data_to_add["vehicle_ownership_status"] = np.where(
        data_to_add["author_name"].isin(authors),
        data_to_add["author_name"].map(author_to_status),
        ""
    )

    data_to_add["ownership_count"] = data_to_add["author_name"].map(ownership_counts).fillna(0).astype(int)

    data_to_add = (
        data_to_add
        .assign(__rank=data_to_add["vehicle_ownership_status"].map(STATUS_ORDER).fillna(0))
        .sort_values("__rank", ascending=False)
        .drop_duplicates(subset=["post_text", "feedback_subcategory"], keep="first")
        .drop(columns="__rank")
    )

    keep_idx = [_a1_col_to_index(c) for c in VISIBLE_COLS]
    max_cols = max(len(data_to_add.columns), max(keep_idx) + 1)
    sheet.overwrite_tab(vehicle_name, data_to_add)
    sheet.hide_columns_except(vehicle_name, VISIBLE_COLS, max_cols)
    sheet.reset_row_heights(vehicle_name)

    print(f"Pasted all posts into {vehicle_name} tab written by its verified owners!")

print(f"Populated: {sheet.url}")
with open(PROJECT_ROOT / "recent_spreadsheet_link.txt", "w") as f:
    f.write(sheet.url)
