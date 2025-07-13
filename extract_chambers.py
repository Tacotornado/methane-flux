import duckdb
import pandas as pd
import os
from datetime import datetime, timedelta

def extract_chamber_active_periods(con, output_dir="chamber_data"):
    """
    Extract chamber data during active periods (ChamberStatus != 0.0) 
    plus 5 seconds before and after each active period.
    Save each chamber's data to CSV and Excel files.
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Find all active periods for each chamber
    print("Finding active periods for each chamber...")
    
    active_periods_query = """
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
        session_starts AS (
            SELECT 
                ChamberID,
                TIMESTAMP AS start_time
            FROM active_sessions
            WHERE ChamberStatus != 0.0 AND (prev_status = 0.0 OR prev_status IS NULL)
        ),
        session_ends AS (
            SELECT 
                ChamberID,
                TIMESTAMP AS end_time
            FROM active_sessions
            WHERE ChamberStatus != 0.0 AND (next_status = 0.0 OR next_status IS NULL)
        )
        SELECT 
            s.ChamberID,
            s.start_time,
            e.end_time,
            DATEDIFF('second', CAST(s.start_time AS TIMESTAMP), CAST(e.end_time AS TIMESTAMP)) AS duration_seconds
        FROM session_starts s
        JOIN session_ends e ON s.ChamberID = e.ChamberID
        WHERE CAST(e.end_time AS TIMESTAMP) > CAST(s.start_time AS TIMESTAMP)
        ORDER BY s.ChamberID, s.start_time
        """

    
    active_periods = con.execute(active_periods_query).fetchall()
    
    print(f"Found {len(active_periods)} active periods")
    
    # Step 2: Extract data for each active period with buffer
    all_chamber_data = {}
    session_count = 0
    
    for chamber_id, start_time, end_time, duration in active_periods:
        session_count += 1
        
        # Convert to datetime objects for calculation
        start_dt = datetime.fromisoformat(str(start_time))
        end_dt = datetime.fromisoformat(str(end_time))
        
        # Add 5 second buffer before and after
        buffer_start = start_dt - timedelta(seconds=5)
        buffer_end = end_dt + timedelta(seconds=5)
        
        print(f"Processing Chamber {chamber_id}, Session {session_count}")
        print(f"  Active period: {start_time} to {end_time} ({duration:.0f} seconds)")
        print(f"  With buffer: {buffer_start} to {buffer_end}")
        
        # Query data for this period with buffer
        data_query = f"""
        SELECT 
            TIMESTAMP, 
            ChamberID, 
            ChamberStatus, 
            ChamberTC, 
            CH4,
            CO2,
            N2O,
            H2O_LI7810,
            H2O_LI7820,
            PPFD,
            PS01
        FROM JAAR_raw 
        WHERE ChamberID = {chamber_id}
        AND TIMESTAMP >= '{buffer_start}'
        AND TIMESTAMP <= '{buffer_end}'
        ORDER BY TIMESTAMP
        """
        
        df = con.execute(data_query).df()
        
        if len(df) > 0:
            # Add metadata columns
            df['session_number'] = session_count
            df['active_start'] = start_time
            df['active_end'] = end_time
            df['duration_seconds'] = duration
            df['is_active'] = df['ChamberStatus'] != 0.0
            
            # Store in dictionary
            session_key = f"Chamber_{int(chamber_id)}_Session_{session_count}"
            all_chamber_data[session_key] = df
            
            # Save individual session files
            csv_filename = os.path.join(output_dir, f"{session_key}.csv")
            excel_filename = os.path.join(output_dir, f"{session_key}.xlsx")
            
            df.to_csv(csv_filename, index=False)
            df.to_excel(excel_filename, index=False)
            
            print(f"  Saved {len(df)} records to {csv_filename} and {excel_filename}")
        else:
            print(f"  No data found for this period")
    
    # Step 3: Create summary files
    print("\nCreating summary files...")
    
    # Summary of all sessions
    summary_data = []
    for session_key, df in all_chamber_data.items():
        summary_data.append({
            'session_key': session_key,
            'chamber_id': df['ChamberID'].iloc[0],
            'session_number': df['session_number'].iloc[0],
            'active_start': df['active_start'].iloc[0],
            'active_end': df['active_end'].iloc[0],
            'duration_seconds': df['duration_seconds'].iloc[0],
            'total_records': len(df),
            'active_records': len(df[df['is_active']]),
            'avg_ch4': df['CH4'].mean(),
            'avg_co2': df['CO2'].mean(),
            'avg_temp': df['ChamberTC'].mean(),
            'min_timestamp': df['TIMESTAMP'].min(),
            'max_timestamp': df['TIMESTAMP'].max()
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(os.path.join(output_dir, "session_summary.csv"), index=False)
    summary_df.to_excel(os.path.join(output_dir, "session_summary.xlsx"), index=False)
    
    # Combined data file for each chamber
    chambers = summary_df['chamber_id'].unique()
    for chamber_id in chambers:
        chamber_sessions = [data for key, data in all_chamber_data.items() 
                          if data['ChamberID'].iloc[0] == chamber_id]
        
        if chamber_sessions:
            combined_df = pd.concat(chamber_sessions, ignore_index=True)
            combined_csv = os.path.join(output_dir, f"Chamber_{int(chamber_id)}_all_sessions.csv")
            combined_excel = os.path.join(output_dir, f"Chamber_{int(chamber_id)}_all_sessions.xlsx")
            
            combined_df.to_csv(combined_csv, index=False)
            combined_df.to_excel(combined_excel, index=False)
            
            print(f"Combined data for Chamber {chamber_id}: {len(combined_df)} records")
    
    print(f"\nExtraction complete!")
    print(f"Total sessions processed: {len(all_chamber_data)}")
    print(f"Files saved to: {output_dir}/")
    print(f"Summary file: {output_dir}/session_summary.csv")
    
    return all_chamber_data, summary_df

# Usage
if __name__ == "__main__":
    # Connect to your database
    con = duckdb.connect(database="dataset/ODB.duckdb")
    
    # Extract data
    chamber_data, summary = extract_chamber_active_periods(con)
    
    # Display summary
    print("\n" + "="*50)
    print("EXTRACTION SUMMARY")
    print("="*50)
    print(summary)
    
    # Close connection
    con.close()