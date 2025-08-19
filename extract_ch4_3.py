import pandas as pd
import duckdb
import os
from datetime import datetime, timedelta
import argparse
from tqdm import tqdm

def extract_chamber_ch4(con, result_dir):
    # Create result directory if it doesn't exist
    os.makedirs(result_dir, exist_ok=True)
    
    # Improved query to find active sessions (when ChamberStatus = 1.0)
    fetch_all_sessions = """
    WITH status_changes AS (
        SELECT
            ChamberID,
            TIMESTAMP,
            ChamberStatus,
            LAG(ChamberStatus) OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS prev_status,
            LEAD(ChamberStatus) OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS next_status,
            ROW_NUMBER() OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS row_num
        FROM JAAR_raw
        ORDER BY ChamberID, TIMESTAMP
    ),
    session_boundaries AS (
        SELECT
            ChamberID,
            TIMESTAMP,
            ChamberStatus,
            prev_status,
            next_status,
            CASE 
                WHEN ChamberStatus = 1.0 AND (prev_status != 1.0 OR prev_status IS NULL) THEN 'START'
                WHEN ChamberStatus = 1.0 AND (next_status != 1.0 OR next_status IS NULL) THEN 'END'
                ELSE NULL
            END AS boundary_type
        FROM status_changes
        WHERE ChamberStatus = 1.0 
        AND ((prev_status != 1.0 OR prev_status IS NULL) OR (next_status != 1.0 OR next_status IS NULL))
    ),
    session_starts AS (
        SELECT
            ChamberID,
            TIMESTAMP AS start_time,
            ROW_NUMBER() OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS session_num
        FROM session_boundaries
        WHERE boundary_type = 'START'
    ),
    session_ends AS (
        SELECT
            ChamberID,
            TIMESTAMP AS end_time,
            ROW_NUMBER() OVER (PARTITION BY ChamberID ORDER BY TIMESTAMP) AS session_num
        FROM session_boundaries
        WHERE boundary_type = 'END'
    )
    SELECT
        s.ChamberID,
        s.start_time,
        e.end_time,
        s.session_num,
        EXTRACT(EPOCH FROM (CAST(e.end_time AS TIMESTAMP) - CAST(s.start_time AS TIMESTAMP))) AS duration_seconds
    FROM session_starts s
    JOIN session_ends e ON s.ChamberID = e.ChamberID AND s.session_num = e.session_num
    WHERE s.start_time < e.end_time
    ORDER BY s.ChamberID, s.start_time
    """
    
    print("Fetching all active sessions...")
    try:
        all_active_sessions = con.execute(fetch_all_sessions).df()
        print(f"Found {len(all_active_sessions)} active sessions")
    except Exception as e:
        print(f"Error fetching sessions: {e}")
        return

    if len(all_active_sessions) == 0:
        print("No active sessions found.")
        return

    # Process each session
    session_count = 0
    skipped_count = 0
    
    for i in tqdm(range(len(all_active_sessions)), desc="Processing active sessions"):
        row = all_active_sessions.iloc[i]
        
        # Extract data from the row
        chamber_id = row['ChamberID']
        start_time = row['start_time']
        end_time = row['end_time']
        session_num = row['session_num']
        duration_seconds = row['duration_seconds']
        
        # Convert to datetime objects for better handling
        try:
            if isinstance(start_time, str):
                start_time_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            else:
                start_time_dt = pd.to_datetime(start_time)
                
            if isinstance(end_time, str):
                end_time_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            else:
                end_time_dt = pd.to_datetime(end_time)
        except Exception as e:
            print(f"Error parsing timestamps for session {i}: {e}")
            skipped_count += 1
            continue

        # Query data for this specific session
        data_query = """
        SELECT
            TIMESTAMP,
            ChamberID,
            ChamberStatus,
            ChamberTC,
            CH4
        FROM JAAR_raw
        WHERE ChamberID = ?
        AND TIMESTAMP >= ?
        AND TIMESTAMP <= ?
        ORDER BY TIMESTAMP
        """
        
        try:
            df = con.execute(data_query, [chamber_id, start_time, end_time]).df()
            
            # Save the dataframe to a CSV file
            if len(df) > 0:
                session_count += 1
                
                # Create a more descriptive filename
                start_str = start_time_dt.strftime("%Y%m%d_%H%M%S")
                session_key = f"Chamber_{int(chamber_id)}_Session_{session_num}_{start_str}"
                csv_filename = os.path.join(result_dir, f"{session_key}.csv")
                
                # Add session metadata as comments in the CSV
                with open(csv_filename, 'w') as f:
                    f.write(f"# Chamber ID: {chamber_id}\n")
                    f.write(f"# Session Number: {session_num}\n")
                    f.write(f"# Start Time: {start_time}\n")
                    f.write(f"# End Time: {end_time}\n")
                    f.write(f"# Duration: {duration_seconds:.1f} seconds\n")
                    f.write(f"# Data Points: {len(df)}\n")
                    f.write("#\n")
                
                # Append the actual data
                df.to_csv(csv_filename, mode='a', index=False)
                
                print(f"Saved: {session_key}.csv ({len(df)} records, {duration_seconds:.1f}s)")
                
            else:
                print(f"No data found for ChamberID {chamber_id}, session {session_num}. Skipping...")
                skipped_count += 1
                
        except Exception as e:
            print(f"Error processing session {i}: {e}")
            skipped_count += 1
            continue

    print(f"\n=== Summary ===")
    print(f"Total sessions found: {len(all_active_sessions)}")
    print(f"Successfully processed: {session_count}")
    print(f"Skipped due to errors: {skipped_count}")
    print(f"CSV files saved to: {result_dir}")


def validate_database(con):
    """Validate that the database contains the expected table and columns."""
    try:
        # Check if table exists
        tables = con.execute("SHOW TABLES").df()
        if 'JAAR_raw' not in tables['name'].values:
            print("Error: Table 'JAAR_raw' not found in database")
            return False
            
        # Check columns
        columns = con.execute("DESCRIBE JAAR_raw").df()
        expected_cols = ['TIMESTAMP', 'ChamberID', 'ChamberStatus', 'ChamberTC', 'CH4']
        missing_cols = [col for col in expected_cols if col not in columns['column_name'].values]
        
        if missing_cols:
            print(f"Error: Missing columns in JAAR_raw: {missing_cols}")
            return False
            
        # Check data range
        summary = con.execute("""
            SELECT 
                COUNT(*) as total_records,
                MIN(TIMESTAMP) as min_time,
                MAX(TIMESTAMP) as max_time,
                COUNT(DISTINCT ChamberID) as num_chambers,
                SUM(CASE WHEN ChamberStatus = 1.0 THEN 1 ELSE 0 END) as active_records
            FROM JAAR_raw
        """).df()
        
        print(f"Database validation successful:")
        print(f"  Total records: {summary.iloc[0]['total_records']:,}")
        print(f"  Time range: {summary.iloc[0]['min_time']} to {summary.iloc[0]['max_time']}")
        print(f"  Number of chambers: {summary.iloc[0]['num_chambers']}")
        print(f"  Active status records: {summary.iloc[0]['active_records']:,}")
        
        return True
        
    except Exception as e:
        print(f"Database validation failed: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract chamber sessions when ChamberStatus changes from 0.0 to 1.0 and back to 0.0"
    )
    parser.add_argument("--db", type=str, default="dataset/JAAR_DB.duckdb", 
                       help="Path to DuckDB database file")
    parser.add_argument("--result_dir", type=str, default="result", 
                       help="Directory to save CSV files")
    parser.add_argument("--validate", action="store_true", 
                       help="Validate database before processing")
    
    args = parser.parse_args()

    # Check if database file exists
    if not os.path.exists(args.db):
        print(f"Error: Database file '{args.db}' not found")
        exit(1)

    try:
        con = duckdb.connect(database=args.db, read_only=True)
        print(f"Connected to database: {args.db}")
        
        if args.validate:
            if not validate_database(con):
                exit(1)
        
        extract_chamber_ch4(con, args.result_dir)
        
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)
    finally:
        if 'con' in locals():
            con.close()