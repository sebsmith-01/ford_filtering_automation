import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import os
import pandas as pd
from helper_functions import get_monday_str

# TODO: Refactor using polars

def main():
    monday_str = get_monday_str()
    data_path = PROJECT_ROOT / f"weekly_data/{monday_str}"

    for path in os.listdir(data_path):
        if path.startswith("full_data"):
            weekly_data = pd.read_excel(f"{data_path}/{path}", sheet_name="Sheet1")
            cols = list(weekly_data.columns)
            cols[35] = "id-2"   # column AJ
            cols[55] = "id-3"   # column BD
            weekly_data.columns = cols
        elif path.startswith("dataset"):
            facebook_data = pd.read_excel(f"{data_path}/{path}", sheet_name="Data")

    name_by_url = (
        facebook_data.dropna(subset=["url", "user/name"])
        .drop_duplicates(subset=["url"], keep="first")
        .set_index("url")["user/name"]
        .to_dict()
    )

    facebook_mask = (weekly_data["data_source"] == "Facebook Groups")
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
        "Infotainment": "Infotainment/Centerstack",
    }
    weekly_data["feedback_subcategory"] = weekly_data["feedback_subcategory"].replace(tagging_corrections)

    # Adding 'Validation' column next to translated text
    weekly_data.insert(11, "Validation", "")

    # When ownership_status is blank, fill from ownership_second
    weekly_data["ownership_status"] = weekly_data["ownership_status"].astype(object)
    weekly_data.loc[weekly_data["ownership_status"].isna(), "ownership_status"] = weekly_data.loc[
        weekly_data["ownership_status"].isna(), "ownership_second"
    ]
    weekly_data["model_comparison"] = weekly_data["model_comparison"].astype(object)
    weekly_data.loc[weekly_data["model_comparison"].isna(), "model_comparison"] = "False"
    weekly_data["is_malfunction"] = weekly_data["is_malfunction"].astype(object)
    weekly_data.loc[~weekly_data["is_malfunction"].isin([True, False]), "is_malfunction"] = "False"

    # Check whether image and engagements values have been edited
    if (weekly_data['fb_comments'].nunique() == 1):
        engagements_by_url = (
            facebook_data.dropna(subset=["url", "commentsCount"])
            .drop_duplicates(subset=["url"], keep="first")
            .set_index("url")["commentsCount"]
            .to_dict()
        )
        weekly_data.loc[facebook_mask, "fb_comments"] = weekly_data.loc[facebook_mask, "url"].map(engagements_by_url)

    if (weekly_data['has_image_link'].nunique() == 1):
        weekly_data["has_image_link"] = weekly_data["has_image_link"].astype(object)
        has_image_by_url = (
            facebook_data.drop_duplicates(subset=["url"], keep="first")
            .set_index("url")["attachments/0/__typename"]
            .apply(lambda x: "True" if pd.notna(x) and str(x).strip() != "" else "False")
            .to_dict()
        )
        weekly_data.loc[facebook_mask, "has_image_link"] = weekly_data.loc[facebook_mask, "url"].map(has_image_by_url)

    weekly_data.to_excel(data_path / f"added_facebook_names_{monday_str}.xlsx", index=False)


if __name__ == "__main__":
    main()
