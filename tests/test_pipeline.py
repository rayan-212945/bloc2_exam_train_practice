"""
========================================================
FICHIER : test_pipeline.py
OBJECTIF :
Tests simples et utiles pour le pipeline E2

Ce fichier vérifie :
- la présence des fichiers bruts ;
- la lecture minimale des sources ;
- la présence des colonnes clés ;
- l'existence de la table analytique finale ;
- la cohérence minimale de fact_stock_risk.
========================================================
"""

import os
import pandas as pd


# ========================================================
# CONFIGURATION DES CHEMINS (ROBUSTE)
# ========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

RAW_PATH = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(PROJECT_DIR, "data", "processed")


# ========================================================
# TEST 1 :
# Vérifier que les fichiers bruts existent
# ========================================================

def test_raw_files_exist():
    expected_files = [
        "orders.csv",
        "products.csv",
        "stores.csv",
        "inventory.json",
        "reviews.jsonl",
        "events_api_sample.json",
    ]

    for file_name in expected_files:
        full_path = os.path.join(RAW_PATH, file_name)
        assert os.path.exists(full_path), f"Fichier manquant : {file_name}"


# ========================================================
# TEST 2 :
# Vérifier les colonnes essentielles de orders.csv
# ========================================================

def test_orders_columns():
    df = pd.read_csv(os.path.join(RAW_PATH, "orders.csv"))

    expected_columns = {
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "channel",
    }

    assert expected_columns.issubset(df.columns), \
        "Colonnes manquantes dans orders.csv"


# ========================================================
# TEST 3 :
# Vérifier les colonnes essentielles de products.csv
# ========================================================

def test_products_columns():
    df = pd.read_csv(os.path.join(RAW_PATH, "products.csv"))

    expected_columns = {
        "product_id",
        "product_name",
        "category",
        "brand",
        "cost_price",
        "sale_price",
    }

    assert expected_columns.issubset(df.columns), \
        "Colonnes manquantes dans products.csv"


# ========================================================
# TEST 4 :
# Vérifier les colonnes essentielles de stores.csv
# ========================================================

def test_stores_columns():
    df = pd.read_csv(os.path.join(RAW_PATH, "stores.csv"))

    expected_columns = {
        "store_id",
        "store_name",
        "city",
        "region",
        "opening_date",
    }

    assert expected_columns.issubset(df.columns), \
        "Colonnes manquantes dans stores.csv"


# ========================================================
# TEST 5 :
# Vérifier que la table analytique finale existe
# ========================================================

def test_fact_stock_risk_file_exists():
    output_file = os.path.join(PROCESSED_PATH, "fact_stock_risk.csv")
    assert os.path.exists(output_file), \
        "Le fichier fact_stock_risk.csv n'existe pas. Lance d'abord etl.py"


# ========================================================
# TEST 6 :
# Vérifier les colonnes attendues de la table finale
# ========================================================

def test_fact_stock_risk_columns():
    output_file = os.path.join(PROCESSED_PATH, "fact_stock_risk.csv")
    df = pd.read_csv(output_file)

    expected_columns = {
        "product_id",
        "store_id",
        "sales_7d",
        "sales_30d",
        "avg_rating",
        "stock_qty",
        "category",
        "region",
        "web_signal",
        "stockout_risk",
    }

    assert expected_columns.issubset(df.columns), \
        "Colonnes manquantes dans fact_stock_risk.csv"


# ========================================================
# TEST 7 :
# Vérifier qu'il n'y a pas de doublons sur la granularité métier
# ========================================================

def test_fact_stock_risk_no_duplicate_keys():
    output_file = os.path.join(PROCESSED_PATH, "fact_stock_risk.csv")
    df = pd.read_csv(output_file)

    duplicates = df.duplicated(subset=["product_id", "store_id"]).sum()

    assert duplicates == 0, \
        f"Doublons détectés sur (product_id, store_id) : {duplicates}"


# ========================================================
# TEST 8 :
# Vérifier que stockout_risk est bien binaire
# ========================================================

def test_stockout_risk_is_binary():
    output_file = os.path.join(PROCESSED_PATH, "fact_stock_risk.csv")
    df = pd.read_csv(output_file)

    valid_values = set(df["stockout_risk"].dropna().unique())

    assert valid_values.issubset({0, 1}), \
        f"Valeurs invalides dans stockout_risk : {valid_values}"


# ========================================================
# TEST 9 :
# Vérifier que les variables numériques principales sont non négatives
# ========================================================

def test_main_numeric_columns_are_non_negative():
    output_file = os.path.join(PROCESSED_PATH, "fact_stock_risk.csv")
    df = pd.read_csv(output_file)

    numeric_columns = ["sales_7d", "sales_30d", "stock_qty", "web_signal"]

    for col in numeric_columns:
        assert (df[col] >= 0).all(), \
            f"Valeurs négatives détectées dans {col}"