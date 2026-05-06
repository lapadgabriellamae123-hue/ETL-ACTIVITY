import sqlite3
import pandas as pd
import os
from sqlalchemy import create_engine # 1. Import SQLAlchemy

TRANSFORM_DB = "data/Transformation/transformation.db"

# 2. Add your Render PostgreSQL connection string
# Note: I added ?sslmode=require at the end, which Render usually needs!
RENDER_DB_URL = "postgresql://db_nako_user:3halkU77mrx0Gaw8HxpVqoDGDDNSDCpc@dpg-d7ngs4vlk1mc73d4p9m0-a.singapore-postgres.render.com/db_nako?sslmode=require"

def build_big_table():
    """Joins sales and items into one consolidated Big Table."""
    os.makedirs("data/Presentation", exist_ok=True) 
    
    # Connect to local SQLite for your staging/transformed data
    t_conn = sqlite3.connect(TRANSFORM_DB)
    
    # 3. Create the SQLAlchemy Engine for Render
    p_engine = create_engine(RENDER_DB_URL)

    # 1. Load Transformed Data
    jp_s = pd.read_sql("SELECT * FROM trf_jp_sales", t_conn)
    jp_i = pd.read_sql("SELECT id, product_name, product_category, unit_price_usd FROM trf_jp_items", t_conn)
    
    mm_s = pd.read_sql("SELECT * FROM trf_mm_sales", t_conn)
    mm_i = pd.read_sql("SELECT id, product_name, product_category, unit_price_usd FROM trf_mm_items", t_conn)

    # 2. Join Sales with Items
    jp_final = jp_s.merge(jp_i, left_on="product_id", right_on="id").drop(columns=["id"])
    jp_final["store"] = "Japan"

    mm_final = mm_s.merge(mm_i, left_on="product_id", right_on="id").drop(columns=["id"])
    mm_final["store"] = "Myanmar"

    # 3. Create Big Table
    big_table = pd.concat([jp_final, mm_final], ignore_index=True)
    big_table["total_revenue_usd"] = big_table["quantity"] * big_table["unit_price_usd"]

    # 4. Save to Presentation Database on Render
    # 4a. Swap 'p_conn' for 'p_engine' here
    big_table.to_sql("fact_global_sales", p_engine, if_exists="replace", index=False)
    
    print("[LOAD] Consolidated BIG TABLE successfully saved to Render PostgreSQL.")
    
    # 5. Clean up connections
    t_conn.close()
    p_engine.dispose() # Properly close the SQLAlchemy engine

if __name__ == "__main__":
    build_big_table()