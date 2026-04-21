"""
========================================================
SCRIPT : etl.py
OBJECTIF :
Étape 4 — Conception des procédures ETL (C8)
Étape 5 — Transformation et qualité des données (C9 / C10)

Ce script doit :
- charger les données utiles depuis la zone raw ;
- nettoyer et normaliser les sources ;
- construire les agrégats nécessaires ;
- réaliser les jointures ;
- produire la table analytique finale fact_stock_risk ;
- enregistrer cette table dans data/processed.
========================================================
"""

import os
import json
from datetime import timedelta

import pandas as pd


# ========================================================
# CONFIGURATION DES CHEMINS (ROBUSTE)
# ========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

RAW_PATH = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(PROJECT_DIR, "data", "processed")
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "etl.log")

os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(os.path.join(PROJECT_DIR, "logs"), exist_ok=True)


# ========================================================
# ETAPE 4 — QUESTION A :
# Décrire / implémenter les étapes de l’ETL
#
# Ici, on sépare bien :
# 1. extraction
# 2. transformation
# 3. chargement
# ========================================================

def load_csv(file_name):
    """Charge un fichier CSV."""
    path = os.path.join(RAW_PATH, file_name)
    return pd.read_csv(path)


def load_json(file_name):
    """Charge un fichier JSON classique."""
    path = os.path.join(RAW_PATH, file_name)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.DataFrame(data)


def load_jsonl(file_name):
    """Charge un fichier JSONL (une ligne JSON par enregistrement)."""
    path = os.path.join(RAW_PATH, file_name)
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    return pd.DataFrame(records)


# ========================================================
# ETAPE 4 — QUESTION B :
# Identifier les données de référence et les relations
#
# Données de référence :
# - products : référentiel produit
# - stores   : référentiel magasin
#
# Données de faits :
# - orders
# - inventory
# - reviews
# - events
#
# Relations principales :
# - product_id relie commandes, produits, stock, avis, événements
# - store_id relie commandes, magasins et stock
# - la granularité finale = couple (product_id, store_id)
# ========================================================


# ========================================================
# ETAPE 4 — QUESTION C :
# Préciser les règles de nettoyage, normalisation et agrégation
#
# On applique ici :
# - conversion des dates
# - normalisation des catégories / régions
# - suppression des doublons
# - gestion des valeurs manquantes minimales
# - calcul des ventes récentes
# - calcul de avg_rating
# - calcul du dernier stock connu
# - calcul d’un signal web simple
# ========================================================

def normalize_text_columns(df, columns):
    """
    Nettoyage texte simple :
    - strip
    - lower
    - remplacement éventuel des valeurs nulles par chaîne vide
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
    return df


def clean_orders(df):
    """
    Nettoyage de orders.csv
    Points d’attention du sujet :
    - doublons possibles
    - dates potentiellement hétérogènes
    - valeurs manquantes
    """
    df = df.copy()

    # Suppression des doublons exacts
    df = df.drop_duplicates()

    # Conversion des dates
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Conversion des types numériques
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # Suppression des lignes sans clés minimales
    df = df.dropna(subset=["order_id", "product_id", "store_id", "order_date"])

    # Quantités négatives ou nulles : on garde seulement les quantités strictement positives
    df = df[df["quantity"] > 0]

    return df


def clean_products(df):
    """
    Nettoyage de products.csv
    Le sujet signale que les catégories peuvent être mal saisies / hétérogènes.
    """
    df = df.copy()

    df = df.drop_duplicates(subset=["product_id"])
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")

    # Normalisation simple des catégories et marques
    df = normalize_text_columns(df, ["category", "brand", "product_name"])

    df = df.dropna(subset=["product_id"])

    return df


def clean_stores(df):
    """
    Nettoyage de stores.csv
    Le sujet signale une homogénéisation possible de region.
    """
    df = df.copy()

    df = df.drop_duplicates(subset=["store_id"])
    df["opening_date"] = pd.to_datetime(df["opening_date"], errors="coerce")

    df = normalize_text_columns(df, ["store_name", "city", "region"])

    df = df.dropna(subset=["store_id"])

    return df


def clean_inventory(df):
    """
    Nettoyage de inventory.json
    Règle métier clé :
    on conserve le dernier stock connu pour chaque couple product_id, store_id.
    """
    df = df.copy()

    df = df.drop_duplicates()
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce")
    df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce")

    df = df.dropna(subset=["product_id", "store_id", "last_update"])

    # Si stock incohérent, on borne au minimum à 0
    df["stock_qty"] = df["stock_qty"].fillna(0)
    df["stock_qty"] = df["stock_qty"].clip(lower=0)

    # On garde la dernière mise à jour par couple produit-magasin
    df = df.sort_values("last_update")
    df = df.drop_duplicates(subset=["product_id", "store_id"], keep="last")

    return df


def clean_reviews(df):
    """
    Nettoyage de reviews.jsonl
    Usage attendu :
    calcul d’un indicateur simple comme avg_rating par produit.
    """
    df = df.copy()

    df = df.drop_duplicates(subset=["review_id"])
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    # On ne garde que les notes valides entre 1 et 5
    df = df[(df["rating"] >= 1) & (df["rating"] <= 5)]

    df = df.dropna(subset=["product_id", "rating"])

    return df


def clean_events(df):
    """
    Nettoyage de events_api_sample.json
    Usage attendu :
    signal complémentaire de demande numérique.
    """
    df = df.copy()

    df = df.drop_duplicates(subset=["event_id"])
    df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")

    df = normalize_text_columns(df, ["event_type", "source"])

    # On ne garde que les types attendus
    valid_event_types = {"view", "cart", "wishlist"}
    df = df[df["event_type"].isin(valid_event_types)]

    df = df.dropna(subset=["event_id", "product_id", "event_ts"])

    return df


# ========================================================
# ETAPE 4 — QUESTION D :
# Détailler les contrôles qualité à mettre en place
#
# Ici, on crée quelques contrôles simples :
# - nb de lignes
# - nb de doublons restants
# - nb de nulls sur clés importantes
# - export d’un fichier quality_checks.csv
# ========================================================

def quality_summary(df, dataset_name, key_columns):
    """
    Retourne un résumé qualité pour un dataset donné.
    """
    summary = {
        "dataset": dataset_name,
        "rows": len(df),
        "columns": len(df.columns),
        "duplicates": int(df.duplicated().sum()),
    }

    for col in key_columns:
        summary[f"null_{col}"] = int(df[col].isna().sum()) if col in df.columns else None

    return summary


# ========================================================
# ETAPE 5 — QUESTION A / B / C :
# Nettoyer les données utiles
# Uniformiser formats, types, catégories, identifiants
# Gérer doublons et valeurs manquantes
# ========================================================

def build_sales_features(orders):
    """
    Construit sales_7d et sales_30d par couple (product_id, store_id).
    """
    max_date = orders["order_date"].max()

    # Fenêtres récentes
    date_7d = max_date - timedelta(days=7)
    date_30d = max_date - timedelta(days=30)

    sales_7d = (
        orders[orders["order_date"] >= date_7d]
        .groupby(["product_id", "store_id"], as_index=False)["quantity"]
        .sum()
        .rename(columns={"quantity": "sales_7d"})
    )

    sales_30d = (
        orders[orders["order_date"] >= date_30d]
        .groupby(["product_id", "store_id"], as_index=False)["quantity"]
        .sum()
        .rename(columns={"quantity": "sales_30d"})
    )

    # Fusion des deux agrégats
    sales = pd.merge(sales_30d, sales_7d, on=["product_id", "store_id"], how="outer")
    sales["sales_7d"] = sales["sales_7d"].fillna(0).astype(int)
    sales["sales_30d"] = sales["sales_30d"].fillna(0).astype(int)

    return sales


def build_reviews_features(reviews):
    """
    Construit avg_rating par produit.
    """
    agg_reviews = (
        reviews.groupby("product_id", as_index=False)
        .agg(
            avg_rating=("rating", "mean"),
            nb_reviews=("review_id", "count"),
            last_review_date=("review_date", "max")
        )
    )
    return agg_reviews


def build_web_signal(events):
    """
    Construit un indicateur simple d’activité web récente par produit.
    Ici on compte le nombre d’événements des 30 derniers jours.
    """
    max_ts = events["event_ts"].max()
    date_30d = max_ts - timedelta(days=30)

    web_signal = (
        events[events["event_ts"] >= date_30d]
        .groupby("product_id", as_index=False)
        .size()
        .rename(columns={"size": "web_signal"})
    )

    return web_signal


# ========================================================
# ETAPE 5 — QUESTION D / E :
# Réaliser les jointures nécessaires
# Construire la table analytique finale fact_stock_risk
# ========================================================

def build_fact_stock_risk(orders, products, stores, inventory, reviews, events):
    """
    Construit la table analytique finale fact_stock_risk.
    """

    # 1. Agrégats de demande
    sales = build_sales_features(orders)

    # 2. Agrégats avis
    agg_reviews = build_reviews_features(reviews)

    # 3. Signal web
    web_signal = build_web_signal(events)

    # 4. Réduction des référentiels
    dim_product = products[["product_id", "category"]].copy()
    dim_store = stores[["store_id", "region"]].copy()

    # 5. Jointure centrale sur la granularité métier product_id + store_id
    df = sales.merge(
        inventory[["product_id", "store_id", "stock_qty"]],
        on=["product_id", "store_id"],
        how="left"
    )
    df = df.merge(dim_product, on="product_id", how="left")
    df = df.merge(dim_store, on="store_id", how="left")
    df = df.merge(agg_reviews[["product_id", "avg_rating"]], on="product_id", how="left")
    df = df.merge(web_signal, on="product_id", how="left")

    # Valeurs manquantes par défaut
    df["avg_rating"] = df["avg_rating"].fillna(0)
    df["web_signal"] = df["web_signal"].fillna(0).astype(int)
    df["stock_qty"] = df["stock_qty"].fillna(0).astype(int)

    # ====================================================
    # ETAPE 7 — Préparation anticipée de la cible métier
    # Ici, on applique une définition pédagogique simple :
    # stockout_risk = 1 si stock faible ET demande récente élevée
    #
    # Cette logique est cohérente avec le sujet et l’annexe.
    # Elle peut être ajustée selon les données réelles.
    # ====================================================
    seuil_stock = 10
    seuil_demande = 5

    df["stockout_risk"] = (
        (df["stock_qty"] < seuil_stock) &
        (df["sales_7d"] > seuil_demande)
    ).astype(int)

    # Colonnes finales attendues
    final_columns = [
        "product_id",
        "store_id",
        "sales_7d",
        "sales_30d",
        "avg_rating",
        "stock_qty",
        "category",
        "region",
        "web_signal",
        "stockout_risk"
    ]

    df = df[final_columns].copy()

    return df


# ========================================================
# ETAPE 5 — QUESTION F :
# Mettre en place des vérifications d’intégrité et de cohérence
# ========================================================

def check_final_table(df):
    """
    Contrôles simples sur la table finale.
    """
    print("\n=== CONTROLES TABLE FINALE ===")
    print("Nombre de lignes :", len(df))
    print("Colonnes :", df.columns.tolist())
    print("Nulls par colonne :")
    print(df.isna().sum())

    # Contrôle de granularité
    duplicates = df.duplicated(subset=["product_id", "store_id"]).sum()
    print("Doublons sur (product_id, store_id) :", duplicates)

    # Distribution de la cible
    print("Distribution stockout_risk :")
    print(df["stockout_risk"].value_counts(dropna=False))


# ========================================================
# CHARGEMENT DES SORTIES
# ========================================================

def save_outputs(fact_stock_risk, quality_checks):
    """
    Sauvegarde des fichiers transformés.
    """
    fact_stock_risk.to_csv(os.path.join(PROCESSED_PATH, "fact_stock_risk.csv"), index=False)
    quality_checks.to_csv(os.path.join(PROCESSED_PATH, "quality_checks.csv"), index=False)

    # Log minimal
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write("ETL exécuté avec succès.\n")
        f.write(f"fact_stock_risk rows: {len(fact_stock_risk)}\n")


# ========================================================
# PIPELINE ETL GLOBAL
# ========================================================

def run_etl():
    """
    Exécute le pipeline ETL complet.
    """

    print("=== ETL START ===")

    # 1. Extraction
    orders_raw = load_csv("orders.csv")
    products_raw = load_csv("products.csv")
    stores_raw = load_csv("stores.csv")
    inventory_raw = load_json("inventory.json")
    reviews_raw = load_jsonl("reviews.jsonl")
    events_raw = load_json("events_api_sample.json")

    # 2. Nettoyage
    orders = clean_orders(orders_raw)
    products = clean_products(products_raw)
    stores = clean_stores(stores_raw)
    inventory = clean_inventory(inventory_raw)
    reviews = clean_reviews(reviews_raw)
    events = clean_events(events_raw)

    # 3. Contrôles qualité intermédiaires
    quality_rows = [
        quality_summary(orders, "orders", ["order_id", "product_id", "store_id"]),
        quality_summary(products, "products", ["product_id"]),
        quality_summary(stores, "stores", ["store_id"]),
        quality_summary(inventory, "inventory", ["product_id", "store_id"]),
        quality_summary(reviews, "reviews", ["review_id", "product_id"]),
        quality_summary(events, "events", ["event_id", "product_id"]),
    ]
    quality_checks = pd.DataFrame(quality_rows)

    # 4. Construction de la table analytique
    fact_stock_risk = build_fact_stock_risk(
        orders=orders,
        products=products,
        stores=stores,
        inventory=inventory,
        reviews=reviews,
        events=events
    )

    # 5. Contrôles finaux
    check_final_table(fact_stock_risk)

    # 6. Chargement des sorties
    save_outputs(fact_stock_risk, quality_checks)

    print("\n✔ ETL terminé avec succès.")
    print(fact_stock_risk.head())


# ========================================================
# POINT D’ENTRÉE
# ========================================================

if __name__ == "__main__":
    run_etl()