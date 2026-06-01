import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

START_DATE = '2026-02-01'
END_DATE = '2026-03-30'

output_name = "ford_models_query.xlsx"

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")

ENGINE = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

query = f"""
SELECT
    dm.*, -- All columns from data_mention
    da.name AS author_name, -- Author's name
    dt.title AS thread_title, -- Thread's title
    dv.*, -- All columns from data_vehiclemention
    df.*, -- All columns from data_feedback
    dm.id as mentionid,
    dv.id as id2,
    df.id as id3
FROM
    data_mention dm
JOIN
    data_author da ON dm.author_id = da.id
JOIN
    data_thread dt ON dm.thread_id = dt.id
JOIN
    data_vehiclemention dv ON dv.mention_id = dm.id
JOIN
    data_feedback df ON df.vehicle_mention_id = dv.id
WHERE
    dm.post_date BETWEEN '{START_DATE}' AND '{END_DATE}'
    AND (
        dv.model IN ('Kuga', 'Explorer', 'Capri', 'Puma') OR
        dv.desired_vehicle_id IN (1, 7, 11, 20)
    );
"""

df = pd.read_sql_query(query, con=ENGINE)

data_outputs = PROJECT_ROOT / "data_outputs"
if not data_outputs.exists():
    data_outputs.mkdir()

output_path = data_outputs / output_name
df.to_excel(output_path, index=False)
print(f"Saved {len(df)} rows to {output_path}")
