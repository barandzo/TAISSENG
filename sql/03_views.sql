-- Vue denormalisee : evite de reecrire les 4 jointures dans chaque requete.
-- Sert de socle a l API et a un futur dashboard Grafana ou Power BI.
CREATE OR REPLACE VIEW v_sales_detail AS
SELECT f.transaction_id,
       d.full_date,
       d.year, d.month, d.month_name, d.day_name, d.is_weekend,
       s.store_id, s.store_name, s.city   AS store_city,
       p.product_id, p.product_name, p.category,
       c.customer_id, c.customer_name, c.customer_type,
       f.quantity, f.unit_price, f.total_amount,
       f.transaction_ts, f.source_file
FROM fact_sales f
JOIN dim_date     d ON d.date_key     = f.date_key
JOIN dim_store    s ON s.store_key    = f.store_key
JOIN dim_product  p ON p.product_key  = f.product_key
JOIN dim_customer c ON c.customer_key = f.customer_key;

-- Agregat journalier par magasin : granularite typique d un suivi d activite
CREATE OR REPLACE VIEW v_daily_store_sales AS
SELECT d.full_date,
       s.store_id,
       s.store_name,
       COUNT(*)            AS transactions,
       SUM(f.quantity)     AS units_sold,
       SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_date  d ON d.date_key  = f.date_key
JOIN dim_store s ON s.store_key = f.store_key
GROUP BY d.full_date, s.store_id, s.store_name;