import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine
import pandas as pd
from helper_functions import get_monday_str
from dotenv import load_dotenv
import os


def get_owners_and_preowners(desired_vehicle_id, engine, vehicle_id_dict):
    desired_vehicle_id = str(desired_vehicle_id)

    query = f"""
    SELECT DISTINCT da.name AS author_name, dv.desired_vehicle_id, dv.ownership_status
FROM data_mention dm
JOIN data_author da
  ON dm.author_id = da.id
JOIN data_thread dt
  ON dm.thread_id = dt.id
JOIN data_vehiclemention dv
  ON dv.mention_id = dm.id
JOIN data_feedback df
  ON df.vehicle_mention_id = dv.id
WHERE dm.post_date BETWEEN '2025-01-01' AND CURRENT_DATE
  AND dv.desired_vehicle_id = {desired_vehicle_id}
  AND df.hit = TRUE
  AND dv.ownership_status IN ('Owner', 'Pre-Ownership', 'Showing Interest')
ORDER BY da.name;
    """

    df = pd.read_sql_query(query, con=engine)
    df["vehicle_name"] = df["desired_vehicle_id"].map(vehicle_id_dict)
    return df  # Note that df can be empty


def main():
    load_dotenv()

    vehicle_ids_df = pd.read_csv(PROJECT_ROOT / "vehicle_ids.csv")
    vehicle_id_dict = dict(zip(vehicle_ids_df["desired_vehicle_id"], vehicle_ids_df["vehicle_name"]))
    monday_str = get_monday_str()

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
    )

    df = pd.DataFrame(columns=["author_name", "desired_vehicle_id", "ownership_status", "vehicle_name"])
    for desired_vehicle_id in vehicle_id_dict.keys():
        print(f"Getting data for desired_vehicle_id: {desired_vehicle_id}")
        vehicle_df = get_owners_and_preowners(desired_vehicle_id, engine, vehicle_id_dict)
        print(f"{vehicle_df.shape[0]} owners for vehicle_id {desired_vehicle_id}")

        # Ensure every vehicle_id gets a row so downstream tabs are not skipped
        if vehicle_df.shape[0] == 0:
            vehicle_df = pd.DataFrame([{
                "author_name": "__placeholder__",
                "desired_vehicle_id": desired_vehicle_id,
                "ownership_status": "",
                "vehicle_name": vehicle_id_dict[desired_vehicle_id],
            }])

        df = pd.concat([df, vehicle_df])

    df.to_csv(PROJECT_ROOT / f"ownership_databases/ownership_database_{monday_str}.csv", index=False)


if __name__ == "__main__":
    main()
