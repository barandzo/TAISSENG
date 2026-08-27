"""Fixtures partagees par les tests."""
import pandas as pd
import pytest

from src.quality import Referentiel


@pytest.fixture
def referentiel():
    """Referentiel reduit, suffisant pour tester les regles."""
    return Referentiel(
        ids_produits={"P001", "P002", "P003"},
        ids_magasins={"S001", "S002"},
        ids_clients={"C001", "C002"},
        prix_catalogue={"P001": 450000, "P002": 520000, "P003": 12000},
    )


def ligne(**overrides):
    """Construit une transaction valide, modifiable par mots-cles."""
    base = {
        "transaction_id": "T000001",
        "store_id": "S001",
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": "2",
        "unit_price": "450000",
        "transaction_date": "2026-08-01 10:30:00",
        "source_file": "test.csv",
    }
    base.update(overrides)
    return base


@pytest.fixture
def make_df():
    """Fabrique un DataFrame a partir de dictionnaires de lignes."""
    def _make(*lignes_dict):
        return pd.DataFrame(list(lignes_dict), dtype=object)
    return _make