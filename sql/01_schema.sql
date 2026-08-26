-- =====================================================================
-- TAISS_PROJECT - Data Warehouse retail - modele en etoile
-- Base cible : warehouse
-- =====================================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER     PRIMARY KEY,
    full_date     DATE        NOT NULL UNIQUE,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    day           SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,
    day_name      VARCHAR(20) NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    is_weekend    BOOLEAN     NOT NULL
);

INSERT INTO dim_date
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER),
    d::DATE,
    EXTRACT(YEAR    FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH   FROM d),
    TRIM(TO_CHAR(d, 'TMMonth')),
    EXTRACT(DAY     FROM d),
    EXTRACT(ISODOW  FROM d),
    TRIM(TO_CHAR(d, 'TMDay')),
    EXTRACT(WEEK    FROM d),
    EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series('2025-01-01'::DATE, '2027-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_product (
    product_key    SERIAL       PRIMARY KEY,
    product_id     VARCHAR(20)  NOT NULL UNIQUE,
    product_name   VARCHAR(150) NOT NULL,
    category       VARCHAR(80),
    catalog_price  NUMERIC(12,2),
    updated_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key   SERIAL       PRIMARY KEY,
    store_id    VARCHAR(20)  NOT NULL UNIQUE,
    store_name  VARCHAR(150) NOT NULL,
    city        VARCHAR(80),
    country     VARCHAR(80),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key   SERIAL       PRIMARY KEY,
    customer_id    VARCHAR(20)  NOT NULL UNIQUE,
    customer_name  VARCHAR(150),
    city           VARCHAR(80),
    customer_type  VARCHAR(50),
    updated_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Membres "inconnu" : evitent de perdre une vente dont la dimension manque
INSERT INTO dim_product (product_id, product_name, category, catalog_price)
VALUES ('UNKNOWN', 'Produit inconnu', 'Inconnu', NULL)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO dim_store (store_id, store_name, city, country)
VALUES ('UNKNOWN', 'Magasin inconnu', NULL, NULL)
ON CONFLICT (store_id) DO NOTHING;

INSERT INTO dim_customer (customer_id, customer_name, city, customer_type)
VALUES ('UNKNOWN', 'Client inconnu', NULL, 'Inconnu')
ON CONFLICT (customer_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key        BIGSERIAL     PRIMARY KEY,
    transaction_id  VARCHAR(30)   NOT NULL UNIQUE,
    date_key        INTEGER       NOT NULL REFERENCES dim_date(date_key),
    store_key       INTEGER       NOT NULL REFERENCES dim_store(store_key),
    product_key     INTEGER       NOT NULL REFERENCES dim_product(product_key),
    customer_key    INTEGER       NOT NULL REFERENCES dim_customer(customer_key),
    quantity        INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    total_amount    NUMERIC(14,2) NOT NULL,
    transaction_ts  TIMESTAMP     NOT NULL,
    source_file     VARCHAR(120)  NOT NULL,
    loaded_at       TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_store    ON fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key);

CREATE TABLE IF NOT EXISTS rejected_transactions (
    reject_id       BIGSERIAL    PRIMARY KEY,
    transaction_id  VARCHAR(30),
    source_file     VARCHAR(120) NOT NULL,
    reject_reason   VARCHAR(120) NOT NULL,
    raw_record      JSONB        NOT NULL,
    rejected_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reject_reason ON rejected_transactions(reject_reason);

CREATE TABLE IF NOT EXISTS etl_file_log (
    file_name      VARCHAR(120) PRIMARY KEY,
    rows_read      INTEGER      NOT NULL,
    rows_loaded    INTEGER      NOT NULL,
    rows_rejected  INTEGER      NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    processed_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);