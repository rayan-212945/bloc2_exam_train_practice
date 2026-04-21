-- ========================================================
-- FICHIER : schema.sql
-- OBJECTIF :
-- Étape 3 — Conception du stockage (C7)
--
-- - Concevoir une base relationnelle pour les entités utiles
-- - Créer les tables SQL
-- - Préparer le stockage pour l'analyse et l'ETL
-- ========================================================


-- ========================================================
-- ETAPE 3 — QUESTION A :
-- Concevoir une base relationnelle pour les entités utiles
--
-- Ici on crée les tables minimales recommandées par le sujet :
-- - dim_product
-- - dim_store
-- - fact_orders
-- - fact_inventory
-- - fact_stock_risk
-- ========================================================


-- ========================================================
-- TABLE DE DIMENSION : PRODUITS
-- Source : products.csv
-- Rôle : référentiel produit
-- ========================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    cost_price REAL,
    sale_price REAL
);


-- ========================================================
-- TABLE DE DIMENSION : MAGASINS
-- Source : stores.csv
-- Rôle : référentiel magasin
-- ========================================================
CREATE TABLE IF NOT EXISTS dim_store (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    city TEXT,
    region TEXT,
    opening_date TEXT
);


-- ========================================================
-- TABLE DE FAITS : COMMANDES
-- Source : orders.csv
-- Rôle : historique des ventes
-- ========================================================
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    order_date TEXT,
    customer_id TEXT,
    product_id TEXT,
    store_id TEXT,
    quantity INTEGER,
    unit_price REAL,
    channel TEXT,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
);


-- ========================================================
-- TABLE DE FAITS : INVENTAIRE
-- Source : inventory.json
-- Rôle : dernière quantité connue par couple produit-magasin
-- ========================================================
CREATE TABLE IF NOT EXISTS fact_inventory (
    product_id TEXT,
    store_id TEXT,
    stock_qty INTEGER,
    last_update TEXT,
    PRIMARY KEY (product_id, store_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
);


-- ========================================================
-- TABLE OPTIONNELLE MAIS UTILE : EVENEMENTS WEB
-- Source : events_api_sample.json
-- Rôle : signal complémentaire de demande digitale
-- ========================================================
CREATE TABLE IF NOT EXISTS fact_events (
    event_id TEXT PRIMARY KEY,
    product_id TEXT,
    event_type TEXT,
    event_ts TEXT,
    source TEXT,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);


-- ========================================================
-- TABLE OPTIONNELLE MAIS UTILE : AVIS AGREGEGÉS
-- Source : reviews.jsonl
-- Rôle : indicateurs simples sur les avis
-- ========================================================
CREATE TABLE IF NOT EXISTS agg_reviews (
    product_id TEXT PRIMARY KEY,
    avg_rating REAL,
    nb_reviews INTEGER,
    last_review_date TEXT,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);


-- ========================================================
-- ETAPE 3 — QUESTION C :
-- Créer la table analytique finale
--
-- Cette table sera alimentée après l'ETL.
-- Elle sert pour l'analyse et l'entraînement ML.
-- ========================================================
CREATE TABLE IF NOT EXISTS fact_stock_risk (
    product_id TEXT,
    store_id TEXT,
    sales_7d INTEGER,
    sales_30d INTEGER,
    avg_rating REAL,
    stock_qty INTEGER,
    category TEXT,
    region TEXT,
    web_signal INTEGER,
    stockout_risk INTEGER,
    PRIMARY KEY (product_id, store_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
);