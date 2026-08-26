"""Etape 3 - Transformation et calcul des mesures."""
import os
import logging

import pandas as pd

from src.config import STAGING_DIR

log = logging.getLogger(__name__)

COLONNES_FINALES = [
    "transaction_id", "store_id", "customer_id", "product_id",
    "quantity", "unit_price", "total_amount",
    "transaction_ts", "date_key", "source_file",
]


def transform_data(**context):
    ti = context["ti"]
    ts = context["ts_nodash"]
    chemin_clean = ti.xcom_pull(task_ids="validate_data", key="clean_path")

    if not chemin_clean:
        ti.xcom_push(key="final_path", value="")
        return 0

    df = pd.read_csv(chemin_clean, encoding="utf-8")
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"])

    df["quantity"] = df["quantity_num"].astype(int)
    df["unit_price"] = df["unit_price_num"].astype(float).round(2)
    df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    df["date_key"] = df["transaction_ts"].dt.strftime("%Y%m%d").astype(int)

    final = df[COLONNES_FINALES]
    chemin_final = os.path.join(STAGING_DIR, "final_" + ts + ".csv")
    final.to_csv(chemin_final, index=False, encoding="utf-8", header=False)

    log.info("%d ligne(s) transformee(s) | CA du lot : %.2f",
             len(final), final["total_amount"].sum())

    ti.xcom_push(key="final_path", value=chemin_final)
    return len(final)