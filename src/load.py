"""Etapes 0 et 4 - Chargement des dimensions puis de la table de faits."""
import os
import logging

import pandas as pd

from src.config import REF_DIR
from src.db import get_conn

log = logging.getLogger(__name__)


def load_dimensions(**_):
    """Upsert des dimensions depuis les fichiers de reference."""
    produits = pd.read_csv(os.path.join(REF_DIR, "products.csv"), encoding="utf-8")
    magasins = pd.read_csv(os.path.join(REF_DIR, "stores.csv"), encoding="utf-8")
    clients = pd.read_csv(os.path.join(REF_DIR, "customers.csv"), encoding="utf-8")

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO dim_product (product_id, product_name, category, catalog_price) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (product_id) DO UPDATE SET "
            "product_name = EXCLUDED.product_name, category = EXCLUDED.category, "
            "catalog_price = EXCLUDED.catalog_price, updated_at = NOW()",
            produits[["product_id", "product_name", "category", "unit_price"]].values.tolist(),
        )
        cur.executemany(
            "INSERT INTO dim_store (store_id, store_name, city, country) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (store_id) DO UPDATE SET "
            "store_name = EXCLUDED.store_name, city = EXCLUDED.city, "
            "country = EXCLUDED.country, updated_at = NOW()",
            magasins[["store_id", "store_name", "city", "country"]].values.tolist(),
        )
        cur.executemany(
            "INSERT INTO dim_customer (customer_id, customer_name, city, customer_type) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (customer_id) DO UPDATE SET "
            "customer_name = EXCLUDED.customer_name, city = EXCLUDED.city, "
            "customer_type = EXCLUDED.customer_type, updated_at = NOW()",
            clients[["customer_id", "customer_name", "city", "customer_type"]].values.tolist(),
        )
        conn.commit()

    log.info("Dimensions a jour : %d produits, %d magasins, %d clients",
             len(produits), len(magasins), len(clients))


def load_warehouse(**context):
    ti = context["ti"]
    chemin_final = ti.xcom_pull(task_ids="transform_data", key="final_path")
    fichiers = ti.xcom_pull(task_ids="ingest_data", key="files") or []
    lues = ti.xcom_pull(task_ids="ingest_data", key="rows_read_by_file") or {}
    rejetees = ti.xcom_pull(task_ids="validate_data", key="rejected_by_file") or {}

    if not chemin_final:
        log.info("Rien a charger.")
        return 0

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE stg_transactions;")
        with open(chemin_final, "r", encoding="utf-8") as fh:
            cur.copy_expert("COPY stg_transactions FROM STDIN WITH (FORMAT csv)", fh)
        cur.execute("SELECT COUNT(*) FROM stg_transactions;")
        en_staging = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO fact_sales (
                transaction_id, date_key, store_key, product_key, customer_key,
                quantity, unit_price, total_amount, transaction_ts, source_file)
            SELECT s.transaction_id, s.date_key, st.store_key, p.product_key, c.customer_key,
                   s.quantity, s.unit_price, s.total_amount, s.transaction_ts, s.source_file
            FROM stg_transactions s
            JOIN dim_date     d  ON d.date_key    = s.date_key
            JOIN dim_store    st ON st.store_id   = s.store_id
            JOIN dim_product  p  ON p.product_id  = s.product_id
            JOIN dim_customer c  ON c.customer_id = COALESCE(s.customer_id, 'UNKNOWN')
            ON CONFLICT (transaction_id) DO NOTHING;
        """)
        inseres = cur.rowcount

        # Compte reel par fichier, lu depuis la table de faits elle-meme
        cur.execute(
            "SELECT source_file, COUNT(*) FROM fact_sales "
            "WHERE source_file = ANY(%s) GROUP BY source_file",
            (fichiers,),
        )
        charges = dict(cur.fetchall())

        for nom in fichiers:
            cur.execute(
                "INSERT INTO etl_file_log "
                "(file_name, rows_read, rows_loaded, rows_rejected, status) "
                "VALUES (%s, %s, %s, %s, 'SUCCESS') "
                "ON CONFLICT (file_name) DO UPDATE SET "
                "rows_read = EXCLUDED.rows_read, rows_loaded = EXCLUDED.rows_loaded, "
                "rows_rejected = EXCLUDED.rows_rejected, status = 'SUCCESS', "
                "processed_at = NOW()",
                (nom, lues.get(nom, 0), charges.get(nom, 0), rejetees.get(nom, 0)),
            )

        conn.commit()

    log.info("Staging : %d | inserees : %d | fichiers logues : %d",
             en_staging, inseres, len(fichiers))
    return inseres