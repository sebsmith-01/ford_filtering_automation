import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import datetime
import os
from helper_functions import get_monday_str

monday_str = get_monday_str()
data_path = PROJECT_ROOT / f"weekly_data/{monday_str}"
for path in os.listdir(data_path): 
    if path.startswith("full_data"): 
        weekly_data = pd.read_excel(f"{data_path}/{path}", sheet_name="Sheet1")
    elif path.startswith("dataset"): 
        facebook_data = pd.read_excel(f"{data_path}/{path}",  sheet_name="Data") 

name_by_url = (
    facebook_data.dropna(subset=['url', 'user/name'])
    .drop_duplicates(subset=['url'], keep='first')
    .set_index("url")["user/name"]
    .to_dict()
)

# name_by_url = (
#     facebook_data.dropna(subset=['facebookUrl', 'user/name'])
#     .drop_duplicates(subset=['facebookUrl'], keep='first')
#     .set_index("facebookUrl")["user/name"]
#     .to_dict()
# )    

facebook_mask = (weekly_data['data_source'] == 'Facebook Groups')
weekly_data.loc[facebook_mask, "author_name"] = weekly_data.loc[facebook_mask, "url"].map(name_by_url)

# Set author_name = "author {mention_id}" for anonymous users
anonymous_mask = (
    weekly_data["author_name"].fillna("")
    .str.strip().str.lower().str.startswith("anonymous")
)

weekly_data.loc[anonymous_mask, "author_name"] = (
    "author " + weekly_data.loc[anonymous_mask, "id"].astype(str)
)

# Correct subcategory tags
tagging_corrections = {
    "Overall Satisfaction With the Car": "Overall Satisfaction with the Vehicle", 
    "Overall Satisfaction with the Car": "Overall Satisfaction with the Vehicle",
    "Range": "Range/Consumption", 
    "Switching to Another Brand": "Switching to Another Vehicle",
    "Overall Disappointment With the Brand": "Overall Satisfaction with the Brand", 
    "Communication From Brand": "Communication from Brand", 
    "Aftersales (OEM App Support)": "OEM App Support Team",
    "Owners Manual": "Owner's Manual", 
    "Communication With Dealer": "Communication with Dealer",
    "Consumption/Range": "Range/Consumption",
    "Infotainment": "Infotainment/Centerstack"
}
weekly_data["feedback_subcategory"] = weekly_data["feedback_subcategory"].replace(tagging_corrections) 
# Adding 'Validation' column next to translated text
weekly_data.insert(11, 'Validation', '')
# When ownership_status is blank, fill from ownership_first
weekly_data.loc[weekly_data['ownership_status'].isna(), 'ownership_status'] = weekly_data.loc[weekly_data['ownership_status'].isna(), 'ownership_second']
weekly_data.loc[weekly_data['model_comparison'].isna(), 'model_comparison'] = 'False'
weekly_data.loc[~weekly_data['is_malfunction'].isin([True, False]), 'is_malfunction'] = 'False'

weekly_data.to_excel(data_path / f"added_facebook_names_{monday_str}.xlsx", index=False)
