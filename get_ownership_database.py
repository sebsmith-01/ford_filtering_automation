from sqlalchemy import create_engine
import pandas as pd
import datetime
from helper_functions import get_monday_str
from dotenv import load_dotenv
import os

load_dotenv()

vehicle_ids_df = pd.read_csv('vehicle_ids.csv')
vehicle_id_dict = vehicle_dict = dict(zip(vehicle_ids_df["desired_vehicle_id"], vehicle_ids_df["vehicle_name"]))
monday_str = get_monday_str()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")

ENGINE = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# Assuming that engine has already been defined
def get_owners_and_preowners(desired_vehicle_id): 
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
    
    df = pd.read_sql_query(query, con=ENGINE)
    df["vehicle_name"] = df["desired_vehicle_id"].map(vehicle_id_dict)
    return df # Note that df can be empty

def get_owner_database_csv(): 
    df = pd.DataFrame(columns=['author_name', 'desired_vehicle_id', 'ownership_status', 'vehicle_name'])
    for desired_vehicle_id in vehicle_id_dict.keys(): 
        print(f"Getting data for desired_vehicle_id: {desired_vehicle_id}")
        vehicle_df = get_owners_and_preowners(desired_vehicle_id)
        print(f"{vehicle_df.shape[0]} owners for vehicle_id {desired_vehicle_id}")
        # Skip adding to df if no verified owners for vehicle
        if vehicle_df.shape[0] == 0: 
            continue
        df = pd.concat([df, vehicle_df])
    df.to_csv(f"ownership_databases/ownership_database_{monday_str}.csv", index=False)

if __name__ == "__main__": 
    get_owner_database_csv()