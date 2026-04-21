"""
========================================================
SCRIPT : train.py
OBJECTIF :
Étape 7 — Développement d’un algorithme d’IA (C12)

Ce script doit :
- charger la table analytique finale ;
- définir la cible stockout_risk ;
- préparer les variables d'entrée ;
- entraîner un modèle simple, robuste et frugal ;
- évaluer le modèle ;
- sauvegarder le modèle entraîné.
========================================================
"""

import os
import math
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix


# ========================================================
# CONFIGURATION DES CHEMINS (ROBUSTE)
# ========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

INPUT_FILE = os.path.join(PROJECT_DIR, "data", "processed", "fact_stock_risk.csv")
MODEL_PATH = os.path.join(PROJECT_DIR, "models", "model.pkl")
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "train.log")

os.makedirs(os.path.join(PROJECT_DIR, "models"), exist_ok=True)
os.makedirs(os.path.join(PROJECT_DIR, "logs"), exist_ok=True)


# ========================================================
# CIBLE
# ========================================================

TARGET_COLUMN = "stockout_risk"


# ========================================================
# CHARGEMENT DES DONNEES
# ========================================================

def load_data(file_path):
    """
    Charge la table analytique finale produite par l'ETL.
    """
    df = pd.read_csv(file_path)
    return df


def prepare_features(df, target_column):
    """
    Prépare X et y :
    - supprime les lignes sans cible ;
    - sépare variables explicatives et variable cible ;
    - encode les colonnes catégorielles.
    """
    df = df.copy()

    df = df.dropna(subset=[target_column])

    X = df.drop(columns=[target_column])
    y = df[target_column]

    columns_to_drop_if_exist = ["product_id", "store_id"]
    for col in columns_to_drop_if_exist:
        if col in X.columns:
            X = X.drop(columns=[col])

    encoders = {}
    for col in X.select_dtypes(include=["object", "string"]).columns:
        encoder = LabelEncoder()
        X[col] = encoder.fit_transform(X[col].astype(str))
        encoders[col] = encoder

    return X, y, encoders


# ========================================================
# MODELE
# ========================================================

def train_model(X_train, y_train):
    """
    Entraîne un RandomForestClassifier simple.
    """
    model = RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        class_weight="balanced"
    )
    model.fit(X_train, y_train)
    return model


# ========================================================
# CHOIX INTELLIGENT DU TEST_SIZE
# ========================================================

def choose_test_size(y, min_positive_in_test=1, default_test_size=0.2):
    """
    Ajuste automatiquement la taille du jeu de test pour essayer
    d'obtenir au moins min_positive_in_test exemples positifs
    dans le jeu de test.

    Exemple :
    - si seulement 2 positifs existent,
      test_size=0.2 donne souvent 0 positif dans le test ;
    - on augmente alors test_size pour sécuriser l'évaluation.
    """
    class_counts = y.value_counts()

    if len(class_counts) < 2:
        return default_test_size

    minority_count = class_counts.min()

    # Taille minimale théorique pour avoir au moins 1 exemple minoritaire en test
    required_test_size = min_positive_in_test / minority_count

    # On prend le max entre le défaut et la valeur requise
    test_size = max(default_test_size, required_test_size)

    # On borne pour éviter un test trop grand
    test_size = min(test_size, 0.5)

    return test_size


# ========================================================
# EVALUATION
# ========================================================

def evaluate_model(model, X_test, y_test):
    """
    Évalue le modèle sur le jeu de test.
    """
    y_pred = model.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, labels=[0, 1], zero_division=0)
    matrix = confusion_matrix(y_test, y_pred, labels=[0, 1])

    print("\n=== EVALUATION DU MODELE ===")
    print(f"Accuracy : {acc:.4f}")
    print("\nClassification report :")
    print(report)
    print("\nMatrice de confusion :")
    print(matrix)

    return {
        "accuracy": acc,
        "classification_report": report,
        "confusion_matrix": matrix.tolist()
    }


# ========================================================
# SAUVEGARDE
# ========================================================

def save_model(model, encoders, output_path):
    """
    Sauvegarde le modèle et les encodeurs.
    """
    artifact = {
        "model": model,
        "encoders": encoders
    }
    joblib.dump(artifact, output_path)
    print(f"\n✔ Modèle sauvegardé dans : {output_path}")


# ========================================================
# LOG
# ========================================================

def write_log(metrics):
    """
    Journalisation minimale de l'entraînement.
    """
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write("=== TRAIN RUN ===\n")
        f.write(f"Accuracy: {metrics['accuracy']:.4f}\n")
        f.write("Classification report:\n")
        f.write(metrics["classification_report"] + "\n")
        f.write("Confusion matrix:\n")
        f.write(str(metrics["confusion_matrix"]) + "\n\n")


# ========================================================
# PIPELINE COMPLET
# ========================================================

def run_training():
    """
    Pipeline complet :
    1. Charger les données
    2. Préparer X / y
    3. Split train / test
    4. Entraîner le modèle
    5. Évaluer
    6. Sauvegarder
    """

    print("=== TRAIN START ===")

    # 1. Chargement des données
    df = load_data(INPUT_FILE)

    print("Shape dataset :", df.shape)
    print("Colonnes :", df.columns.tolist())

    # 2. Préparation des features
    X, y, encoders = prepare_features(df, TARGET_COLUMN)

    print("Shape X :", X.shape)
    print("Shape y :", y.shape)

    print("\nDistribution de la cible :")
    print(y.value_counts())

    class_counts = y.value_counts()

    # 3. Train / test split
    if y.nunique() > 1 and class_counts.min() >= 2:
        test_size = choose_test_size(y, min_positive_in_test=1, default_test_size=0.2)
        print(f"\nSplit stratifié activé avec test_size={test_size:.2f}")

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=42,
            stratify=y
        )
    else:
        print("\nSplit stratifié désactivé (classes insuffisantes).")
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=None
        )

    print("\nDistribution y_train :")
    print(y_train.value_counts())

    print("\nDistribution y_test :")
    print(y_test.value_counts())

    # 4. Entraînement
    model = train_model(X_train, y_train)

    # 5. Évaluation
    metrics = evaluate_model(model, X_test, y_test)

    # 6. Sauvegarde
    save_model(model, encoders, MODEL_PATH)

    # 7. Log
    write_log(metrics)

    print("\n✔ Entraînement terminé avec succès.")


# ========================================================
# POINT D'ENTRÉE
# ========================================================

if __name__ == "__main__":
    run_training()