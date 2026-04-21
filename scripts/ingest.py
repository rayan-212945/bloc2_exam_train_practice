"""
========================================================
SCRIPT : ingest.py
OBJECTIF :
Étape 2 — Collecte et ingestion des données (C6)

- Charger les fichiers CSV, JSON et JSONL
- Mettre en place des contrôles de lecture
- Journaliser les erreurs
========================================================
"""

import os
import pandas as pd
import json

# ==========================================
# CONFIGURATION DES CHEMINS (ROBUSTE)
# ==========================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

RAW_PATH = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(PROJECT_DIR, "data", "processed")
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "ingest.log")

# Création automatique des dossiers utiles
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(os.path.join(PROJECT_DIR, "logs"), exist_ok=True)

# ==========================================
# ETAPE 2 — QUESTION A :
# Écrire un script d’ingestion en Python
# ==========================================

def load_csv(file_name):
    """
    Charge un fichier CSV avec gestion d’erreur
    """
    try:
        path = os.path.join(RAW_PATH, file_name)
        df = pd.read_csv(path)
        print(f"[OK] Chargement CSV : {file_name}")
        return df
    except Exception as e:
        print(f"[ERREUR] CSV {file_name} : {e}")
        return None


# ==========================================
# ETAPE 2 — QUESTION B :
# Charger les fichiers JSON
# ==========================================

def load_json(file_name):
    """
    Charge un fichier JSON classique
    """
    try:
        path = os.path.join(RAW_PATH, file_name)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        print(f"[OK] Chargement JSON : {file_name}")
        return df
    except Exception as e:
        print(f"[ERREUR] JSON {file_name} : {e}")
        return None


# ==========================================
# ETAPE 2 — QUESTION B (suite) :
# Charger les fichiers JSONL
# ==========================================

def load_jsonl(file_name):
    """
    Charge un fichier JSONL (1 JSON par ligne)
    """
    try:
        path = os.path.join(RAW_PATH, file_name)
        data = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line))
        df = pd.DataFrame(data)
        print(f"[OK] Chargement JSONL : {file_name}")
        return df
    except Exception as e:
        print(f"[ERREUR] JSONL {file_name} : {e}")
        return None


# ==========================================
# ETAPE 2 — QUESTION C :
# Contrôles élémentaires de lecture
# ==========================================

def basic_check(df, name):
    """
    Vérifie :
    - présence du DataFrame
    - nombre de lignes
    - colonnes
    """
    if df is None:
        print(f"[ERREUR] {name} non chargé")
        return False

    print(f"[CHECK] {name}")
    print(f"→ lignes : {df.shape[0]}")
    print(f"→ colonnes : {df.shape[1]}")
    print(df.columns.tolist())

    return True


# ==========================================
# ETAPE 2 — QUESTION D :
# Journalisation simple des erreurs
# ==========================================

def log_error(message):
    """
    Log simple dans un fichier texte
    """
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(message + "\n")


# ==========================================
# PIPELINE D’INGESTION COMPLET
# ==========================================

def run_ingestion():
    """
    Lance toute l’ingestion des sources
    """

    # CSV
    orders = load_csv("orders.csv")
    products = load_csv("products.csv")
    stores = load_csv("stores.csv")

    # JSON
    inventory = load_json("inventory.json")
    events = load_json("events_api_sample.json")

    # JSONL
    reviews = load_jsonl("reviews.jsonl")

    # Vérifications
    datasets = {
        "orders": orders,
        "products": products,
        "stores": stores,
        "inventory": inventory,
        "events": events,
        "reviews": reviews
    }

    for name, df in datasets.items():
        if not basic_check(df, name):
            log_error(f"Erreur sur dataset : {name}")

    # Sauvegarde en processed (optionnel mais recommandé)
    for name, df in datasets.items():
        if df is not None:
            output_file = os.path.join(PROCESSED_PATH, f"{name}.csv")
            df.to_csv(output_file, index=False)

    print("✔ Ingestion terminée")


# ==========================================
# POINT D’ENTRÉE
# ==========================================

if __name__ == "__main__":
    run_ingestion()