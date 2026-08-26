"""Acces PostgreSQL."""
import psycopg2
from src.config import PG_DSN


def get_conn():
    """Retourne une connexion psycopg2 au Data Warehouse."""
    return psycopg2.connect(PG_DSN)


def fetch_processed_files():
    """Fichiers deja charges avec succes."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT file_name FROM etl_file_log WHERE status = 'SUCCESS'")
        return {r[0] for r in cur.fetchall()}