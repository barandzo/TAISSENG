"""Connexion au Data Warehouse via un pool SQLAlchemy."""
import os

from sqlalchemy import create_engine, text

DSN = os.getenv(
    "WAREHOUSE_DSN",
    "postgresql+psycopg2://taiss:taiss@postgres:5432/warehouse",
)

# pool_pre_ping : verifie la connexion avant usage, evite les erreurs
# apres un redemarrage de PostgreSQL
engine = create_engine(DSN, pool_pre_ping=True, pool_size=5, max_overflow=5)


def query(sql, params=None):
    """Execute une requete SELECT et retourne une liste de dictionnaires."""
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
    return [dict(r) for r in rows]


def query_one(sql, params=None):
    res = query(sql, params)
    return res[0] if res else None