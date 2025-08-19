import pandas as pd
import duckdb
import os
from datetime import datetime, timedelta
import argparse
from tqdm import tqdm

def extract_chamber_ch4(con, result_dir):
    # Fetch all of the data
    fetch_all_sessions = """
    WITH active_sessions AS (
        SELECT
            ChamberID,
            TIMESTAMP,
            ChamberStatus,
            LAG(ChamberStatus) OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS prev_status,
            LEAD(ChamberStatus) OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS next_status
        FROM JAAR_raw
        ORDER BY ChamberID, TIMESTAMP
    ),
    session_start AS (
        SELECT
            ChamberID,
            TIMESTAMP AS start_time,
        FROM active_sessions
        WHERE ChamberStatus != 0 AND (prev_status = 0 OR prev_status IS NULL)
    ),
    session_end AS (
        SELECT
            ChamberID,
            TIMESTAMP AS end_time,
        FROM active_sessions
        WHERE ChamberStatus != 0 AND (next_status = 0 OR next_status IS NULL)
    )
    SELECT
        s.ChamberID,
        s.start_time,
        e.end_time,
        DATEDIFF('second', CAST(s.start_time AS TIMESTAMP), CAST(e.end_time AS TIMESTAMP)) AS duration_seconds
    FROM session_start s
    JOIN LATERAL (
        SELECT e.end_time
        FROM session_end e
        WHERE e.ChamberID = s.ChamberID AND e.end_time > s.start_time
        ORDER BY e.end_time
        LIMIT 1
    ) e ON true
    ORDER BY s.ChamberID, s.start_time
    """
    
    print("Fetching all of the active sessions...")
    all_active_sessions = con.execute(fetch_all_sessions).df()
    print(f"Found {len(all_active_sessions)} active sessions")

    # Fetching the data from the found session
    session_count = 0
    _len = len(all_active_sessions)
    for i in tqdm(range(_len), desc="Querying active sessions"):
        row_1 = all_active_sessions.iloc[i]

        #Extract data from the first row
        chamber_id = row_1['ChamberID']
        start_time = row_1['start_time']
        end_time = row_1['end_time']
        
        start_time_dt = datetime.fromisoformat(str(start_time))
        end_time_dt = datetime.fromisoformat(str(end_time))

        #print(f"Processing ChamberID {chamber_id}, session {session_count}")
        #print(f"Period: {str(start_time)} to {str(end_time)} ({duration_seconds:.0f}seconds)")

        data_query = f"""
        SELECT
            TIMESTAMP,
            CH4,
            ChamberStatus,
            ChamberID,
            ChamberTC,
        FROM JAAR_raw
        WHERE ChamberID = '{chamber_id}'
        AND TIMESTAMP >= '{start_time_dt}'
        AND TIMESTAMP <= '{end_time_dt}'
        ORDER BY TIMESTAMP
        """

        df = con.execute(data_query).df()
        
        #Save the dataframe to a CSV file
        if len(df) > 0:
            session_count += 1
            session_key = f"Chamber_{int(chamber_id)}_Session_{i}"
            csv_filename = os.path.join(result_dir, f"{session_key}.csv")
            df.to_csv(csv_filename, index=False)
        else:
            print(f"No data found for ChamberID {chamber_id} in session {i}. Skipping...")

    print(f"Total sessions processed: {session_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default="dataset/JAAR_DB.duckdb")
    parser.add_argument("--result_dir", type=str, default="result")
    args = parser.parse_args()

    con = duckdb.connect(database=args.db, read_only=True)
    result_dir = args.result_dir
    extract_chamber_ch4(con, result_dir)