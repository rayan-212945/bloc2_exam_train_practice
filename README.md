# 📊 RetailPulse — Pipeline Data & Machine Learning

## 🎯 Objectif
Pipeline complet permettant :
- ingestion multi-sources (CSV, JSON, JSONL)
- transformation (ETL)
- création de la table analytique `fact_stock_risk`
- entraînement d’un modèle ML
- stockage PostgreSQL via Docker
- validation par tests

---

## ⚙️ Stack technique
Python 3.12 • Docker • PostgreSQL • pgAdmin • Pandas • Matplotlib • SQLAlchemy • Scikit-learn • Joblib • Pytest

---

## 🏗️ Structure du projet

```
.
├── README.md  
├── data  
│   ├── processed  
│   │   ├── events.csv  
│   │   ├── fact_stock_risk.csv  
│   │   ├── inventory.csv  
│   │   ├── orders.csv  
│   │   ├── products.csv  
│   │   ├── quality_checks.csv  
│   │   ├── reviews.csv  
│   │   ├── risk_distribution.png  
│   │   ├── sales_by_region.png  
│   │   ├── sales_vs_stock.png  
│   │   └── stores.csv  
│   └── raw  
│       ├── events_api_sample.json  
│       ├── inventory.json  
│       ├── orders.csv  
│       ├── products.csv  
│       ├── readme_source.txt  
│       ├── reviews.jsonl  
│       └── stores.csv  
├── docker-compose.yml  
├── logs  
│   ├── etl.log  
│   └── train.log  
├── models  
│   └── model.pkl  
├── scripts  
│   ├── db.py  
│   ├── etl.py  
│   ├── ingest.py  
│   ├── ingest_db.py  
│   ├── train.py  
│   └── visualize.py  
├── sql  
│   ├── queries.sql  
│   └── schema.sql  
└── tests  
    ├── __pycache__  
    │   └── test_pipeline.cpython-312-pytest-9.0.3.pyc  
    └── test_pipeline.py  

```
---

## 🚀 Pipeline

docker compose up -d  
python scripts/db.py  
python scripts/ingest.py  
python scripts/etl.py  
python scripts/visualize.py  
python scripts/train.py  
python scripts/ingest_db.py  
pytest -v  

---

## 📊 Résultats

ETL :
- 115 lignes  
- 0 valeurs manquantes  
- 0 doublons  

Règle métier :
stockout_risk = 1 si stock_qty < 10 ET sales_7d > 5 sinon 0  

Distribution :
- 0 → 113  
- 1 → 2  

---

## 🤖 Modèle ML

Dataset :
- 115 lignes  
- 7 features  

Split :
- stratifié  
- test_size = 0.5  

Distribution :
- train → 56 / 1  
- test → 57 / 1  

Résultat :
Accuracy = 0.9828  

Matrice :
[[57 0]  
 [ 1 0]]  

✔ Modèle sauvegardé : models/model.pkl  

---

## 🧪 Tests

pytest -v  
→ 9 / 9 tests validés  

---

## 🧠 Analyse

Problème :
- fort déséquilibre (2 positifs)

Impact :
- accuracy trompeuse  
- recall nul sur classe critique  

Améliorations :
- enrichir données  
- équilibrage classes  
- nouvelles features  
- validation croisée  
- autres modèles  

---

## 🌱 Impact écologique
pipeline léger • modèle simple • faible coût  

---

## ✅ Conclusion
Pipeline complet fonctionnel, reproductible et validé.  
Base solide pour amélioration et production.