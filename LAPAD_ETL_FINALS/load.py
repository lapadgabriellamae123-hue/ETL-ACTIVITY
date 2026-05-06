import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Get the exact folder where this load.py file lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Build the absolute path to the database
TRANSFORM_DB = os.path.join(BASE_DIR, "data", "Transformation", "transformation.db")
PRESENTATION_DIR = os.path.join(BASE_DIR, "data", "Presentation")

# 1. Load the hidden URL from your .env file
load_dotenv()
RENDER_DB_URL = os.getenv("RENDER_DB_URL")

def build_big_table():
    """Joins sales and items into one consolidated Big Table."""
    
    # Use the absolute path for creating the presentation folder
    os.makedirs(PRESENTATION_DIR, exist_ok=True)
    
    # The rest of your code remains exactly the same...
    t_conn = sqlite3.connect(TRANSFORM_DB)
    # ...
    
    # 2. Create the SQLAlchemy Engine for Render
    if RENDER_DB_URL is None:
        print("Error: RENDER_DB_URL not found. Check your .env file!")
        return
        
    p_engine = create_engine(RENDER_DB_URL)

    # 3. Load Transformed Data
    jp_s = pd.read_sql("SELECT * FROM trf_jp_sales", t_conn)
    jp_i = pd.read_sql("SELECT id, product_name, product_category, unit_price_usd FROM trf_jp_items", t_conn)
    
    mm_s = pd.read_sql("SELECT * FROM trf_mm_sales", t_conn)
    mm_i = pd.read_sql("SELECT id, product_name, product_category, unit_price_usd FROM trf_mm_items", t_conn)

    # 4. Join Sales with Items
    jp_final = jp_s.merge(jp_i, left_on="product_id", right_on="id").drop(columns=["id"])
    jp_final["store"] = "Japan"

    mm_final = mm_s.merge(mm_i, left_on="product_id", right_on="id").drop(columns=["id"])
    mm_final["store"] = "Myanmar"

    # 5. Create Big Table
    big_table = pd.concat([jp_final, mm_final], ignore_index=True)
    big_table["total_revenue_usd"] = big_table["quantity"] * big_table["unit_price_usd"]

    # 6. Save to Presentation Database on Render
    try:
        big_table.to_sql("fact_global_sales", p_engine, if_exists="replace", index=False)
        # REMOVE THE EMOJI ON THE LINE BELOW:
        print("[LOAD] Consolidated BIG TABLE successfully saved to Render PostgreSQL!")
    except Exception as e:
        print(f"[ERROR] Failed to save to database: {e}")
    
    # Clean up connections
    t_conn.close()
    p_engine.dispose()

if __name__ == "__main__":
    build_big_table()