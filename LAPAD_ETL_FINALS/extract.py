import pandas as pd
import sqlite3
import os

# Define paths based on your requested structure
DATA_SOURCE = "data/source"
STAGING_DB = "data/Staging/staging.db"

def extract_store(store_name):
    """
    Extract CSV files from a specific store subfolder
    and load them into the staging SQLite database.
    """
    # Ensure the staging directory exists before connecting
    os.makedirs("data/Staging", exist_ok=True)
    conn = sqlite3.connect(STAGING_DB)
    
    # Path to the specific store (e.g., data/source/japan_store)
    store_path = os.path.join(DATA_SOURCE, store_name)

    if not os.path.exists(store_path):
        print(f"[ERROR] Directory not found: {store_path}")
        return

    # Loop through all CSV files in the folder
    for file in os.listdir(store_path):
        if file.endswith(".csv"):
            file_path = os.path.join(store_path, file)
            
            # Read CSV and clean column names (strip quotes/spaces)
            df = pd.read_csv(file_path)
            df.columns = [col.strip("'").strip() for col in df.columns]
            
            # Table name format: storename_filename (e.g., japan_store_sales_data)
            table_name = f"{store_name}_{file.replace('.csv','')}"
            
            # Load into SQL staging
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"[EXTRACT] Loaded table: {table_name}")

    conn.close()

def run_extract():
    """Extract both Japan & Myanmar stores into staging layer."""
    print("--- Starting Extraction Process ---")
    extract_store("japan_store")
    extract_store("myanmar_store")
    print("--- Extraction Complete ---")

if __name__ == "__main__":
    run_extract()