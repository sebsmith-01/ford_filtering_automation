import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine
import pandas as pd
from helper_functions import get_monday_str
from dotenv import load_dotenv
import os


def main():
    load_dotenv()

    vehicle_ids_df = pd.read_csv(PROJECT_ROOT / "vehicle_ids.csv")
    vehicle_id_dict = dict(zip(vehicle_ids_df["desired_vehicle_id"], vehicle_ids_df["vehicle_name"]))
    monday_str = get_monday_str()

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
    )

    ids = tuple(int(i) for i in vehicle_id_dict)
    query = f"""
    SELECT DISTINCT da.name AS author_name, dv.desired_vehicle_id, dv.ownership_status
    FROM data_mention dm
    JOIN data_author da ON dm.author_id = da.id
    JOIN data_vehiclemention dv ON dv.mention_id = dm.id
    JOIN data_feedback df ON df.vehicle_mention_id = dv.id
    WHERE dm.post_date BETWEEN '2025-01-01' AND CURRENT_DATE
      AND dv.desired_vehicle_id IN {ids}
      AND df.hit = TRUE
      AND dv.ownership_status IN ('Owner', 'Pre-Ownership', 'Showing Interest')
    ORDER BY da.name;
    """

    df = pd.read_sql_query(query, con=engine)
    df["vehicle_name"] = df["desired_vehicle_id"].map(vehicle_id_dict)

    # Ensure every vehicle_id gets a row so downstream tabs are not skipped
    present_ids = set(df["desired_vehicle_id"])
    placeholders = [
        {
            "author_name": "__placeholder__",
            "desired_vehicle_id": vid,
            "ownership_status": "",
            "vehicle_name": vehicle_id_dict[vid],
        }
        for vid in vehicle_id_dict
        if vid not in present_ids
    ]
    if placeholders:
        df = pd.concat([df, pd.DataFrame(placeholders)], ignore_index=True)

    df.to_csv(PROJECT_ROOT / f"ownership_databases/ownership_database_{monday_str}.csv", index=False)


if __name__ == "__main__":
    main()
