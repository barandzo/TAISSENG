"""Configuration centralisee : chemins et connexion base."""
import os

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
REF_DIR = os.path.join(DATA_DIR, "reference")
TX_DIR = os.path.join(DATA_DIR, "transactions")
STAGING_DIR = os.path.join(DATA_DIR, "staging")

os.makedirs(STAGING_DIR, exist_ok=True)

_RAW_DSN = os.getenv(
    "WAREHOUSE_DSN",
    "postgresql+psycopg2://taiss:taiss@postgres:5432/warehouse",
)
PG_DSN = _RAW_DSN.replace("postgresql+psycopg2://", "postgresql://")

RAW_COLUMNS = [
    "transaction_id", "store_id", "customer_id", "product_id",
    "quantity", "unit_price", "transaction_date",
]