import os
import pandas as pd
import matplotlib.pyplot as plt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

INPUT_FILE = os.path.join(PROJECT_DIR, "data", "processed", "fact_stock_risk.csv")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "data", "processed")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_visualization():
    df = pd.read_csv(INPUT_FILE)

    print("=== VISUALISATION ===")

    # ==============================
    # 1. Distribution du risque
    # ==============================
    df["stockout_risk"].value_counts().plot(kind="bar")
    plt.title("Distribution du stockout_risk")
    plt.xlabel("Classe")
    plt.ylabel("Nombre")
    plt.savefig(os.path.join(OUTPUT_DIR, "risk_distribution.png"))
    plt.clf()

    # ==============================
    # 2. Sales vs Stock
    # ==============================
    plt.scatter(df["sales_7d"], df["stock_qty"])
    plt.title("Sales 7d vs Stock")
    plt.xlabel("Sales 7d")
    plt.ylabel("Stock")
    plt.savefig(os.path.join(OUTPUT_DIR, "sales_vs_stock.png"))
    plt.clf()

    # ==============================
    # 3. Moyenne des ventes par région
    # ==============================
    df.groupby("region")["sales_30d"].mean().sort_values().plot(kind="barh")
    plt.title("Sales 30d moyen par région")
    plt.xlabel("Sales")
    plt.savefig(os.path.join(OUTPUT_DIR, "sales_by_region.png"))
    plt.clf()

    print("✔ Visualisations générées")


if __name__ == "__main__":
    run_visualization()