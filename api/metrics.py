"""Metriques Prometheus du pipeline retail.

Deux familles de metriques :

  TECHNIQUES  latence, requetes, codes HTTP — fournies automatiquement
              par l instrumentator

  QUALITE     lignes chargees, rejets par motif, fraicheur des donnees —
              definies ici, lues dans PostgreSQL

La seconde famille est la plus importante. Un pipeline peut etre
techniquement vert (CPU normal, API rapide) tout en chargeant des donnees
fausses. Le taux de rejet par magasin est le signal qui revele une caisse
defectueuse en amont, bien avant qu un rapport errone soit lu.

Les valeurs sont rafraichies a chaque collecte (toutes les 15 s par defaut),
pas mises en cache : le cout est negligeable devant l interet d une donnee
a jour.
"""
import logging

from prometheus_client import Gauge

from db import query, query_one

log = logging.getLogger(__name__)


# --------------------------------------------------------------- QUALITE
rows_loaded = Gauge(
    "taiss_rows_loaded_total",
    "Nombre de transactions chargees dans le Data Warehouse",
)

rows_rejected = Gauge(
    "taiss_rows_rejected_total",
    "Nombre de transactions rejetees et tracees",
)

acceptance_rate = Gauge(
    "taiss_acceptance_rate_percent",
    "Pourcentage de lignes acceptees sur le total traite",
)

rejects_by_reason = Gauge(
    "taiss_rejects_by_reason",
    "Nombre de rejets par motif",
    ["reason"],
)

rejects_by_file = Gauge(
    "taiss_rejects_by_source_file",
    "Nombre de rejets par fichier source",
    ["source_file"],
)

# --------------------------------------------------------------- PIPELINE
files_processed = Gauge(
    "taiss_files_processed_total",
    "Nombre de fichiers traites avec succes",
)

data_freshness_hours = Gauge(
    "taiss_data_freshness_hours",
    "Heures ecoulees depuis le dernier chargement",
)

# ----------------------------------------------------------------- METIER
total_revenue = Gauge(
    "taiss_total_revenue",
    "Chiffre d affaires cumule dans le Data Warehouse",
)

revenue_by_store = Gauge(
    "taiss_revenue_by_store",
    "Chiffre d affaires par point de vente",
    ["store_id", "store_name"],
)


def rafraichir():
    """Interroge le Data Warehouse et met a jour toutes les jauges.

    Appelee a chaque collecte Prometheus. Toute erreur est journalisee
    sans etre propagee : un probleme de base ne doit pas faire echouer
    l endpoint /metrics, sinon on perd aussi les metriques techniques.
    """
    try:
        _rafraichir_qualite()
        _rafraichir_pipeline()
        _rafraichir_metier()
    except Exception as exc:
        log.error("Echec du rafraichissement des metriques : %s", exc)


def _rafraichir_qualite():
    charges = query_one("SELECT COUNT(*) AS n FROM fact_sales")["n"]
    rejetes = query_one("SELECT COUNT(*) AS n FROM rejected_transactions")["n"]

    rows_loaded.set(charges)
    rows_rejected.set(rejetes)

    total = charges + rejetes
    acceptance_rate.set(round(100.0 * charges / total, 2) if total else 100.0)

    rejects_by_reason.clear()
    for r in query("""
        SELECT reject_reason, COUNT(*) AS n
        FROM rejected_transactions
        GROUP BY reject_reason
    """):
        rejects_by_reason.labels(reason=r["reject_reason"]).set(r["n"])

    rejects_by_file.clear()
    for r in query("""
        SELECT source_file, COUNT(*) AS n
        FROM rejected_transactions
        GROUP BY source_file
    """):
        rejects_by_file.labels(source_file=r["source_file"]).set(r["n"])


def _rafraichir_pipeline():
    files_processed.set(
        query_one("SELECT COUNT(*) AS n FROM etl_file_log WHERE status = 'SUCCESS'")["n"]
    )

    row = query_one("""
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(loaded_at))) / 3600 AS heures
        FROM fact_sales
    """)
    if row and row["heures"] is not None:
        data_freshness_hours.set(round(float(row["heures"]), 2))


def _rafraichir_metier():
    row = query_one("SELECT COALESCE(SUM(total_amount), 0) AS ca FROM fact_sales")
    total_revenue.set(float(row["ca"]))

    revenue_by_store.clear()
    for r in query("""
        SELECT s.store_id, s.store_name, SUM(f.total_amount) AS ca
        FROM fact_sales f
        JOIN dim_store s ON s.store_key = f.store_key
        GROUP BY s.store_id, s.store_name
    """):
        revenue_by_store.labels(
            store_id=r["store_id"], store_name=r["store_name"]
        ).set(float(r["ca"]))