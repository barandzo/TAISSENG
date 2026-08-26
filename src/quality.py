"""Etape 2 - Validation et nettoyage. Tout rejet est trace en base."""
import os
import json
import logging
from collections import Counter
from datetime import datetime

import pandas as pd

from src.config import REF_DIR, STAGING_DIR, RAW_COLUMNS
from src.db import get_conn

log = logging.getLogger(__name__)

FORMATS_DATE = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y",
)


def _parse_date(valeur):
    """Normalise une date. Retourne None si illisible."""
    if valeur is None or (isinstance(valeur, float) and pd.isna(valeur)):
        return None
    texte = str(valeur).strip()
    if not texte:
        return None
    for fmt in FORMATS_DATE:
        try:
            return datetime.strptime(texte, fmt)
        except ValueError:
            continue
    return None


def _collecter(df, masque, raison, panier):
    """Ecarte les lignes du masque et les met de cote."""
    mauvaises = df[masque]
    if len(mauvaises):
        panier.append((mauvaises.copy(), raison))
        log.warning("  REJET %-32s : %d ligne(s)", raison, len(mauvaises))
    return df[~masque].copy()


def validate_data(**context):
    ti = context["ti"]
    ts = context["ts_nodash"]
    chemin_raw = ti.xcom_pull(task_ids="ingest_data", key="raw_path")

    if not chemin_raw:
        ti.xcom_push(key="clean_path", value="")
        ti.xcom_push(key="rejected_by_file", value={})
        return 0

    df = pd.read_csv(chemin_raw, encoding="utf-8", dtype=str)
    total_lu = len(df)
    log.info("Lignes en entree : %d", total_lu)

    produits = pd.read_csv(os.path.join(REF_DIR, "products.csv"), encoding="utf-8")
    magasins = pd.read_csv(os.path.join(REF_DIR, "stores.csv"), encoding="utf-8")
    clients = pd.read_csv(os.path.join(REF_DIR, "customers.csv"), encoding="utf-8")

    prix_catalogue = dict(zip(produits["product_id"], produits["unit_price"]))
    ids_produits = set(produits["product_id"])
    ids_magasins = set(magasins["store_id"])
    ids_clients = set(clients["customer_id"])

    panier_rejets = []

    avant = len(df)
    df = df.drop_duplicates(subset=RAW_COLUMNS, keep="first")
    log.info("  R1 doublons stricts supprimes   : %d", avant - len(df))

    avant = len(df)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    log.info("  R1 transaction_id dupliques     : %d", avant - len(df))

    df = _collecter(df,
                    df["transaction_id"].isna() | (df["transaction_id"].str.strip() == ""),
                    "transaction_id_manquant", panier_rejets)

    df["transaction_ts"] = df["transaction_date"].apply(_parse_date)
    df = _collecter(df, df["transaction_ts"].isna(), "date_invalide", panier_rejets)

    df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = _collecter(df, df["quantity_num"].isna(), "quantity_non_numerique", panier_rejets)
    df = _collecter(df, df["quantity_num"] <= 0, "quantity_negative_ou_nulle", panier_rejets)

    df = _collecter(df, ~df["product_id"].isin(ids_produits), "produit_inexistant", panier_rejets)
    df = _collecter(df, ~df["store_id"].isin(ids_magasins), "magasin_inexistant", panier_rejets)

    df["unit_price_num"] = pd.to_numeric(df["unit_price"], errors="coerce")
    a_reparer = df["unit_price_num"].isna()
    if a_reparer.any():
        df.loc[a_reparer, "unit_price_num"] = df.loc[a_reparer, "product_id"].map(prix_catalogue)
        log.info("  R6 prix imputes depuis catalogue : %d", int(a_reparer.sum()))
    df = _collecter(df, df["unit_price_num"].isna(), "prix_irrecuperable", panier_rejets)
    df = _collecter(df, df["unit_price_num"] < 0, "prix_negatif", panier_rejets)

    inconnu = df["customer_id"].isna() | ~df["customer_id"].isin(ids_clients)
    if inconnu.any():
        df.loc[inconnu, "customer_id"] = "UNKNOWN"
        log.info("  R7 clients rattaches a UNKNOWN   : %d", int(inconnu.sum()))

    rejets_par_fichier = _ecrire_rejets(panier_rejets)
    nb_rejets = sum(rejets_par_fichier.values())

    chemin_clean = os.path.join(STAGING_DIR, "clean_" + ts + ".csv")
    df.to_csv(chemin_clean, index=False, encoding="utf-8")

    log.info("BILAN : %d lues | %d valides | %d rejetees",
             total_lu, len(df), nb_rejets)

    ti.xcom_push(key="clean_path", value=chemin_clean)
    ti.xcom_push(key="rows_rejected", value=nb_rejets)
    ti.xcom_push(key="rejected_by_file", value=rejets_par_fichier)
    return len(df)


def _ecrire_rejets(panier):
    """Ecrit les rejets en base et retourne le compte par fichier source."""
    if not panier:
        return {}

    cols = RAW_COLUMNS + ["source_file"]
    lignes = []
    compteur = Counter()
    for bloc, raison in panier:
        brut = bloc[cols].where(pd.notna(bloc[cols]), None)
        for enreg in brut.to_dict(orient="records"):
            compteur[enreg.get("source_file")] += 1
            lignes.append((
                enreg.get("transaction_id"),
                enreg.get("source_file"),
                raison,
                json.dumps(enreg, ensure_ascii=False),
            ))

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO rejected_transactions "
            "(transaction_id, source_file, reject_reason, raw_record) "
            "VALUES (%s, %s, %s, %s::jsonb)",
            lignes,
        )
        conn.commit()

    log.info("%d ligne(s) ecrite(s) dans rejected_transactions", len(lignes))
    return dict(compteur)