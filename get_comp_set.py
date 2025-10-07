from sqlalchemy import create_engine
import pandas as pd
from datetime import date
from helper_functions import get_monday_str
from dotenv import load_dotenv
import os

load_dotenv()
# START_DATE = date.today().replace(day=1)
START_DATE = date(2025, 9, 1)
vehicle_ids = pd.read_csv('vehicle_ids.csv')
id_to_name = dict(zip(vehicle_ids["desired_vehicle_id"], vehicle_ids["vehicle_name"]))

# Choose a competitor set here. Must be spelt correctly
competitor_set = "Mustang Mach-E"
include_showing_interest = False
ownerships = "('Owner', 'Pre-Ownership', 'Showing Interest')" if include_showing_interest else "('Owner', 'Pre-Ownership')" 

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")

ENGINE = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

COMPETITOR_SETS = {
    "Mustang Mach-E": [2, 16, 17, 18, 19],
    "Puma Gen-E": [25, 26, 27, 28, 29],
    "Electric Explorer": [1, 2, 3, 4, 5, 6],
    "Ford Capri": [20, 21, 22, 23, 24, 3]
}

comp_set_string = (str(COMPETITOR_SETS.get(competitor_set))).strip('[]') 

query = f"""
SELECT 
    dm.*, -- All columns from data_mention
    da.name AS author_name, -- Author's name
    dt.title AS thread_title, -- Thread's title
    dv.*, -- All columns from data_vehiclemention
    df.* -- All columns from data_feedback
FROM 
    data_mention dm
JOIN 
    data_author da ON dm.author_id = da.id -- data_mention links to data_author
JOIN 
    data_thread dt ON dm.thread_id = dt.id -- data_mention links to data_thread
JOIN 
    data_vehiclemention dv ON dv.mention_id = dm.id -- data_vehiclemention links to data_mention
JOIN 
    data_feedback df ON df.vehicle_mention_id = dv.id -- data_feedback links to data_vehiclemention
WHERE 
    dm.post_date BETWEEN ('{START_DATE}') AND CURRENT_DATE
    AND df.hit = TRUE
    AND dv.desired_vehicle_id IN ({comp_set_string})
    AND dv.ownership_status IN {ownerships};
    """

df = pd.read_sql_query(query, con=ENGINE)
# Editing 'model' so easier to create pivot tables 
df["model"] = df["desired_vehicle_id"].map(id_to_name)
df.to_csv("test.csv", index=False)

