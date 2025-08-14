import pandas as pd
import duckdb
import os
from datetime import datetime, timedelta
import argparse
from tqdm import tqdm

def extract_chamber_ch4(con, result_dir, time_sample):
    
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
    print("Fetching data of the active sessions...")
    session_count = 0
    segment_count = 0
    idx = 0
    dfs = []
    _len = len(all_active_sessions)
    pbar = tqdm(total=_len, desc="Processing session")
    total_duration = 0
    while(True):
        session_count += 1
        row = all_active_sessions.iloc[idx]
        next_row = all_active_sessions.iloc[idx+1] if idx < _len-1 else None

        #Extract data from the first row
        chamber_id = row['ChamberID']
        start_time = row['start_time']
        end_time = row['end_time']
        duration_seconds = row['duration_seconds']
        
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

        # Update some parameter
        idx += 1
        total_duration += duration_seconds
        pbar.update(1)

        # Check if the recent queried df exceed the time sample, if yes split it then append else just append
        if (total_duration >= time_sample):
            time_diff = total_duration - time_sample + 1
            idx_sep = duration_seconds - time_diff
            df_1, df_2 = df.iloc[:idx_sep], df.iloc[idx_sep:]
            dfs.append(df_1)
            dfs.append(df_2)
        else:
            dfs.append(df)

        # Check whether the total duration has exceed the time sample or not
        # Check whether the index has exceed all active sessions
        all_session_processed = (idx >= _len)
        different_id = (row['ChamberID'] != next_row['ChamberID']) if next_row is not None else True
        time_exceed = (total_duration > time_sample)
        time_equal = (total_duration == time_sample)
        if(time_exceed or time_equal or different_id or all_session_processed):
            segment_count += 1
            if time_exceed and not different_id:
                # Combine until dfs[:-1]
                combined_df = pd.concat(dfs[:-1], ignore_index=True)

                # Leave the last element in the dfs
                dfs[:] = [dfs[-1]]

                # Reset the total duration to be the residue time
                total_duration = time_diff
            else:
                # Combine all dfs
                combined_df = pd.concat(dfs, ignore_index=True)

                # Reset the list
                dfs[:] = []

                # Reset the total duration to be 0
                total_duration = 0

                # Reset the segment to 0
                segment_count = 0

            #Save the dataframe to a CSV file
            if len(combined_df) > 0:
                session_key = f"Chamber_{int(chamber_id)}_Segment_{segment_count}"
                csv_filename = os.path.join(result_dir, f"{session_key}.csv")
                combined_df.to_csv(csv_filename, index=False)
            else:
                print(f"No data found for ChamberID {chamber_id} in session {session_count}. Skipping...")
            
            # Break if all session has been processed
            if(all_session_processed):
                break
    
    pbar.close()
    print(f"Total sessions processed: {session_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default="dataset/JAAR_DB.duckdb")
    parser.add_argument("--result_dir", type=str, default="result")
    parser.add_argument("--sample_time", type=int, default=400)
    args = parser.parse_args()

    con = duckdb.connect(database=args.db, read_only=True)
    result_dir = args.result_dir
    sample_time = args.sample_time
    extract_chamber_ch4(con, result_dir, sample_time)