import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from helper_functions import get_monday_str


def main():
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
    if not weekly_data_candidates:
        raise FileNotFoundError("No full_data-*.xlsx file found in Downloads")
    if not facebook_data_candidates:
        raise FileNotFoundError("No matching Facebook groups file found in Downloads")

    weekly_data_path = max(weekly_data_candidates, key=lambda p: p.stat().st_mtime)
    facebook_data_path = max(facebook_data_candidates, key=lambda p: p.stat().st_mtime)

    # Move to path "weekly_data/{date}" for processing
    dest = PROJECT_ROOT / f"weekly_data/{monday_str}"
    dest.mkdir(exist_ok=True)

    os.rename(weekly_data_path, dest / weekly_data_path.name)
    os.rename(facebook_data_path, dest / facebook_data_path.name)


if __name__ == "__main__":
    main()
