"""
========================================================
SCRIPT : ingest_db.py
OBJECTIF :
Charger les données dans PostgreSQL (via SQLAlchemy)
- lecture des fichiers bruts
- nettoyage minimal avant insertion
- purge des tables avant rechargement
- insertion de la table analytique finale
========================================================
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ========================================================
# CONFIGURATION
# ========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

load_dotenv(os.path.join(PROJECT_DIR, ".env"))

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

RAW_PATH = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(PROJECT_DIR, "data", "processed")


# ========================================================
# LECTURE DES SOURCES
# ========================================================

def load_csv_raw(name):
    return pd.read_csv(os.path.join(RAW_PATH, name))

def load_json(name):
    return pd.read_json(os.path.join(RAW_PATH, name))

def load_csv_processed(name):
    return pd.read_csv(os.path.join(PROCESSED_PATH, name))


# ========================================================
# NETTOYAGE MINIMAL AVANT INSERTION
# ========================================================

def clean_products(df):
    return df.drop_duplicates(subset=["product_id"]).copy()

def clean_stores(df):
    return df.drop_duplicates(subset=["store_id"]).copy()

def clean_orders(df):
    df = df.copy()
    df = df.drop_duplicates()
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    return df

def clean_inventory(df):
    df = df.copy()
    df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce")
    df = df.dropna(subset=["product_id", "store_id", "last_update"])
    df = df.sort_values("last_update")
    df = df.drop_duplicates(subset=["product_id", "store_id"], keep="last")
    return df

def clean_fact_stock_risk(df):
    df = df.copy()
    df = df.drop_duplicates(subset=["product_id", "store_id"], keep="first")
    return df


# ========================================================
# PURGE DES TABLES
# ========================================================

def truncate_tables():
    with engine.begin() as conn:
        conn.execute(text("""
            TRUNCATE TABLE
                fact_stock_risk,
                fact_inventory,
                fact_orders,
                dim_store,
                dim_product
            RESTART IDENTITY CASCADE;
        """))
    print("✔ Tables vidées avant rechargement")


# ========================================================
# INSERTION
# ========================================================

def run():
    print("=== INGESTION DB ===")

    # Lecture
    products = load_csv_raw("products.csv")
    stores = load_csv_raw("stores.csv")
    orders = load_csv_raw("orders.csv")
    inventory = load_json("inventory.json")
    fact_stock_risk = load_csv_processed("fact_stock_risk.csv")

    # Nettoyage
    products = clean_products(products)
    stores = clean_stores(stores)
    orders = clean_orders(orders)
    inventory = clean_inventory(inventory)
    fact_stock_risk = clean_fact_stock_risk(fact_stock_risk)

    print(f"Products après nettoyage       : {products.shape}")
    print(f"Stores après nettoyage         : {stores.shape}")
    print(f"Orders après nettoyage         : {orders.shape}")
    print(f"Inventory après nettoyage      : {inventory.shape}")
    print(f"fact_stock_risk après nettoyage: {fact_stock_risk.shape}")

    # Purge avant insertion
    truncate_tables()

    # Insertion
    products.to_sql("dim_product", engine, if_exists="append", index=False)
    stores.to_sql("dim_store", engine, if_exists="append", index=False)
    orders.to_sql("fact_orders", engine, if_exists="append", index=False)
    inventory.to_sql("fact_inventory", engine, if_exists="append", index=False)
    fact_stock_risk.to_sql("fact_stock_risk", engine, if_exists="append", index=False)

    print("✔ Données insérées dans PostgreSQL")


if __name__ == "__main__":
    run()