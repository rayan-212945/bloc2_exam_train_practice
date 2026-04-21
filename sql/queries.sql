-- ========================================================
-- FICHIER : queries.sql
-- OBJECTIF :
-- Vérifier la bonne intégration du stockage PostgreSQL
-- ========================================================


-- ========================================================
-- 1. Vérifier les tables créées dans le schéma public
-- ========================================================
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;


-- ========================================================
-- 2. Compter les lignes par table
-- ========================================================
SELECT COUNT(*) AS nb_products FROM dim_product;
SELECT COUNT(*) AS nb_stores FROM dim_store;
SELECT COUNT(*) AS nb_orders FROM fact_orders;
SELECT COUNT(*) AS nb_inventory FROM fact_inventory;
SELECT COUNT(*) AS nb_stock_risk FROM fact_stock_risk;


-- ========================================================
-- 3. Afficher quelques lignes de chaque table
-- ========================================================
SELECT * FROM dim_product LIMIT 5;
SELECT * FROM dim_store LIMIT 5;
SELECT * FROM fact_orders LIMIT 5;
SELECT * FROM fact_inventory LIMIT 5;
SELECT * FROM fact_stock_risk LIMIT 5;


-- ========================================================
-- 4. Vérifier les doublons potentiels sur les commandes
-- ========================================================
SELECT order_id, COUNT(*) AS nb
FROM fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1;


-- ========================================================
-- 5. Vérifier les valeurs nulles importantes dans fact_orders
-- ========================================================
SELECT COUNT(*) AS null_product_id
FROM fact_orders
WHERE product_id IS NULL;

SELECT COUNT(*) AS null_store_id
FROM fact_orders
WHERE store_id IS NULL;

SELECT COUNT(*) AS null_order_date
FROM fact_orders
WHERE order_date IS NULL;


-- ========================================================
-- 6. Vérifier la table analytique finale
-- ========================================================
SELECT * FROM fact_stock_risk LIMIT 10;

SELECT stockout_risk, COUNT(*) AS nb
FROM fact_stock_risk
GROUP BY stockout_risk
ORDER BY stockout_risk;


-- ========================================================
-- 7. Vérifier les doublons sur la granularité métier finale
-- ========================================================
SELECT product_id, store_id, COUNT(*) AS nb
FROM fact_stock_risk
GROUP BY product_id, store_id
HAVING COUNT(*) > 1;


-- ========================================================
-- 8. Vérifier les moyennes de ventes par région
-- ========================================================
SELECT region, ROUND(AVG(sales_30d), 2) AS avg_sales_30d
FROM fact_stock_risk
GROUP BY region
ORDER BY avg_sales_30d DESC;


-- ========================================================
-- 9. Vérifier les produits / magasins à risque
-- ========================================================
SELECT product_id, store_id, stock_qty, sales_7d, stockout_risk
FROM fact_stock_risk
WHERE stockout_risk = 1
ORDER BY sales_7d DESC, stock_qty ASC;

