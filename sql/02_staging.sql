-- Table tampon : recoit les donnees nettoyees avant resolution des cles
-- de substitution. Permet un chargement ensembliste en SQL.
CREATE TABLE IF NOT EXISTS stg_transactions (
    transaction_id  VARCHAR(30),
    store_id        VARCHAR(20),
    customer_id     VARCHAR(20),
    product_id      VARCHAR(20),
    quantity        INTEGER,
    unit_price      NUMERIC(12,2),
    total_amount    NUMERIC(14,2),
    transaction_ts  TIMESTAMP,
    date_key        INTEGER,
    source_file     VARCHAR(120)
);