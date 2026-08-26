"""Etape 1 - Ingestion. Detection automatique des nouveaux fichiers."""
import os
import glob
import logging

import pandas as pd

from src.config import TX_DIR, STAGING_DIR, RAW_COLUMNS
from src.db import fetch_processed_files

log = logging.getLogger(__name__)


def ingest_data(**context):
    ts = context["ts_nodash"]

    tous = sorted(glob.glob(os.path.join(TX_DIR, "*.csv")))
    deja = fetch_processed_files()
    nouveaux = [f for f in tous if os.path.basename(f) not in deja]

    log.info("Fichiers presents : %d | deja traites : %d | a traiter : %d",
             len(tous), len(deja), len(nouveaux))

    if not nouveaux:
        log.info("Aucun nouveau fichier.")
        context["ti"].xcom_push(key="files", value=[])
        context["ti"].xcom_push(key="raw_path", value="")
        context["ti"].xcom_push(key="rows_read_by_file", value={})
        return 0

    frames = []
    lues_par_fichier = {}
    for chemin in nouveaux:
        nom = os.path.basename(chemin)
        df = pd.read_csv(chemin, encoding="utf-8", dtype=str)
        manquantes = set(RAW_COLUMNS) - set(df.columns)
        if manquantes:
            raise ValueError(nom + " : colonnes manquantes " + str(manquantes))
        df["source_file"] = nom
        frames.append(df)
        lues_par_fichier[nom] = len(df)
        log.info("  %s : %d lignes lues", nom, len(df))

    brut = pd.concat(frames, ignore_index=True)
    chemin_raw = os.path.join(STAGING_DIR, "raw_" + ts + ".csv")
    brut.to_csv(chemin_raw, index=False, encoding="utf-8")

    log.info("TOTAL ingere : %d lignes -> %s", len(brut), chemin_raw)

    context["ti"].xcom_push(key="files", value=[os.path.basename(f) for f in nouveaux])
    context["ti"].xcom_push(key="raw_path", value=chemin_raw)
    context["ti"].xcom_push(key="rows_read", value=len(brut))
    context["ti"].xcom_push(key="rows_read_by_file", value=lues_par_fichier)
    return len(brut)