import pandas as pd
import sqlite3
import os

STAGING_DB = "data/Staging/staging.db"
TRANSFORM_DB = "data/Transformation/transformation.db"

def transform_and_clean():
    """Cleans and standardizes data from staging for the Big Table."""
    os.makedirs("data/Transformation", exist_ok=True)
    
    s_conn = sqlite3.connect(STAGING_DB)
    t_conn = sqlite3.connect(TRANSFORM_DB)

    # 1. Load Sales & Items (Using your new naming convention)
    # Note: Adjust table names if your CSVs are named differently
    jp_sales = pd.read_sql("SELECT * FROM japan_store_sales_data", s_conn)
    jp_items = pd.read_sql("SELECT * FROM japan_store_japan_items", s_conn)
    
    mm_sales = pd.read_sql("SELECT * FROM myanmar_store_sales_data", s_conn)
    mm_items = pd.read_sql("SELECT * FROM myanmar_store_myanmar_items", s_conn)

    # 2. Standardize Japan Data (JPY to USD)
    # Assuming 1 JPY = 0.0067 USD
    jp_items["unit_price_usd"] = jp_items["price"] * 0.0067
    jp_items["product_category"] = jp_items["category"] # Standardize column name
    
    # 3. Standardize Myanmar Data
    # Myanmar items use 'type' and 'name' - renaming to match Japan
    mm_items = mm_items.rename(columns={"type": "product_category", "name": "product_name"})
    mm_items["unit_price_usd"] = mm_items["price"]

    # 4. Save Transformed Items
    jp_items.to_sql("trf_jp_items", t_conn, if_exists="replace", index=False)
    mm_items.to_sql("trf_mm_items", t_conn, if_exists="replace", index=False)
    
    # 5. Save Transformed Sales
    jp_sales.to_sql("trf_jp_sales", t_conn, if_exists="replace", index=False)
    mm_sales.to_sql("trf_mm_sales", t_conn, if_exists="replace", index=False)

    print("[TRANSFORM] Items and Sales standardized.")
    s_conn.close()
    t_conn.close()

if __name__ == "__main__":
    transform_and_clean()