import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
import subprocess
import datetime

load_dotenv()

# Change to TRUE/FALSE based on whether want to use staging or live database
staging = False

user = os.getenv("DB_USER")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
password = os.getenv("DB_PASSWORD")

if staging:
    host = os.getenv("DB_STAGING_HOST")
else:
    host = os.getenv("DB_HOST")

missing = [name for name, val in {"DB_USER": user, "DB_PORT": port, "DB_DATABASE": database, "DB_PASSWORD": password, "DB_STAGING_HOST" if staging else "DB_HOST": host}.items() if not val]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

ENGINE = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

DELETED_LOG = PROJECT_ROOT / "deleted_feedbacks.csv"

get_dupes_query = """
WITH base AS (
	SELECT
	    dm.id,
	    da.name AS author_name,
	    dt.title AS thread_title,
	    dv.desired_vehicle_id,
	    df.*,
		dm.id AS mention_id,
		dv.id AS id2,
		df.id AS id3,
		COUNT(*) OVER (
			PARTITION BY dm.id, dv.desired_vehicle_id, df.feedback_subcategory, df.feedback_sentiment
	    ) AS dup_count
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
	    df.hit = TRUE
)
SELECT *
FROM base
WHERE dup_count > 1
ORDER BY mention_id, desired_vehicle_id, feedback_subcategory, feedback_sentiment;
"""

remove_dupes_query = """
WITH base AS (
	SELECT
	    df.id AS feedback_id,
		ROW_NUMBER() OVER (
			PARTITION BY dm.id, dv.desired_vehicle_id, df.feedback_subcategory, df.feedback_sentiment
			ORDER BY df.id
		) AS row_num
	FROM data_mention dm
	JOIN data_vehiclemention dv ON dv.mention_id = dm.id
	JOIN data_feedback df ON df.vehicle_mention_id = dv.id
	WHERE df.hit = TRUE
)
UPDATE data_feedback
SET hit = FALSE
WHERE id IN (
	SELECT feedback_id
	FROM base
	WHERE row_num > 1
)
RETURNING id
"""

def log_deletions(feedback_ids: list[int]) -> None:
    today = datetime.date.today().isoformat()
    new_rows = pd.DataFrame({"date": today, "feedback_id": feedback_ids})
    if DELETED_LOG.exists():
        new_rows.to_csv(DELETED_LOG, mode="a", header=False, index=False)
    else:
        new_rows.to_csv(DELETED_LOG, index=False)


def main():
    df = pd.read_sql_query(get_dupes_query, con=ENGINE)
    dup_count = len(df)

    # osascript is a MacOS tool. This won't work on a Windows or Linux machine
    if dup_count <= 0:
        subprocess.run([
            'osascript', '-e',
            'display dialog "No duplicates detected in LENS database." buttons {"OK"} default button "OK"'
        ])
        return

    env = "STAGING" if staging else "LIVE"
    result = subprocess.run([
        'osascript', '-e',
        f'display dialog "{dup_count} duplicate rows found in LENS {env} database. Delete them?" '
        f'buttons {{"Cancel", "Delete"}} default button "Cancel"'
    ], capture_output=True, text=True)

    if 'Delete' not in result.stdout:
        print('Cancelled')
        return

    with ENGINE.begin() as conn:
        result = conn.execute(text(remove_dupes_query))
        feedback_ids = [row[0] for row in result]
    log_deletions(feedback_ids)
    print(f"Deleted {len(feedback_ids)} duplicate feedback rows from {env} database. Logged to {DELETED_LOG}")


if __name__ == "__main__":
    main()
