import pandas as pd
from helper_functions import google_sheet_to_dataframe
import sys

# Replace with value for current week (copy and paste from url)
# sheet_id = "1Wt3BLHUSnQahP9QTYVdd1SX7YywfWw6aOXRD6SeDNG0"
sheet_id = "1r14zDB_FN04Tk9Ax3tb3akpX1uEfJZquHmm9m0Ey31A"

# Add more if needed. This is case sensitive 
tagging_corrections = {
    "Overall Satisfaction With the Car": "Overall Satisfaction with the Car", 
    "Range": "Range/Consumption", 
    "Switching to Another Brand": "Switching to Another Vehicle",
    "Overall Disappointment With the Brand": "Overall Satisfaction with the Brand", 
    "Communication From Brand": "Communication from Brand", 
    "Aftersales (OEM App Support)": "OEM App Support Team",
    "Owners Manual": "Owner's Manual", 
    "Communication With Dealer": "Communication with Dealer"
}

valid_sentiments = ["Negative", "Neutral", "Positive"]
valid_ownership = ["Showing Interest", "Pre-Ownership", "Owner"]

vehicle_ids_df = pd.read_csv('vehicle_ids.csv')
vehicle_id_dict = vehicle_dict = dict(zip(vehicle_ids_df["vehicle_name"], vehicle_ids_df["desired_vehicle_id"]))

# List of vehicle tabs with data to upload, edit accordingly
vehicles_to_upload = ["Puma MCA"] 

to_upload = pd.DataFrame()
for vehicle in vehicles_to_upload: 
    all_data = google_sheet_to_dataframe(tab_name=vehicle, sheet_id=sheet_id).loc[:, : "feedback_subcategory"]
    all_data = all_data.assign(
        hit='TRUE',
        desired_vehicle_id = vehicle_id_dict.get(vehicle))
    
    all_data = all_data[all_data['Validation'] == "Hit"]
    
    # Correct Tags
    all_data["feedback_subcategory"] = all_data["feedback_subcategory"].replace(tagging_corrections)
    
    # Check Sentiment
    if not all_data["overall_sentiment"].isin(valid_sentiments).all():
        raise ValueError(f"Invalid value in 'overall_sentiment' column for {vehicle}")
    if not all_data["feedback_sentiment"].isin(valid_sentiments).all():
        raise ValueError(f"Invalid value in 'feedback_sentiment' column for {vehicle}")
    
    # Check Ownership (Assuming that 'Not an Owner' shouldn't be uploaded)
    if not all_data["ownership_status"].isin(valid_ownership).all():
        raise ValueError(f"Invalid value in 'ownership_status' column for {vehicle}")
        
    # Check no more than one id-2 for each id (mention id) in a tab
    if not all_data.groupby("id")["id-2"].nunique().le(1).all():
        raise ValueError(f"More than one id-2 for a single id in {vehicle}")
    
    # Ensure that no repeated rows
    all_data.drop_duplicates(inplace=True)

    to_upload = pd.concat([to_upload, all_data], ignore_index=True)
    
to_upload.to_excel("test1.xlsx", index=False)