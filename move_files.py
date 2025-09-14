import os
from pathlib import Path
import datetime 
from helper_functions import get_monday_str

downloads = Path.home() / "Downloads"
monday_str = get_monday_str()

weekly_data_candidates = [
    p for p in downloads.iterdir()
    if p.is_file()
    and p.suffix.lower() == ".xlsx"
    and p.name.lower().startswith("full_data")
]

facebook_data_candidates = [
    p for p in downloads.iterdir()
    if p.is_file()
    and p.suffix.lower() == ".xlsx" 
    and p.name.lower().startswith("dataset_facebook-groups-scraper")
]

# Return the newest by modified time (i.e., most recently downloaded)
weekly_data_path = max(weekly_data_candidates, key=lambda p: p.stat().st_mtime, default=None)
facebook_data_path = max(facebook_data_candidates, key=lambda p: p.stat().st_mtime, default=None)

# Move to path "weekly_data/{date}" for processing
if not (os.path.exists(os.path.join(os.getcwd(), f"weekly_data/{monday_str}"))):
    os.mkdir(f"weekly_data/{monday_str}")

os.rename(weekly_data_path, f"weekly_data/{monday_str}/{os.path.basename(weekly_data_path)}")
os.rename(facebook_data_path, f"weekly_data/{monday_str}/{os.path.basename(facebook_data_path)}")
