"""Tests des fonctions pures de validation.

Ces fonctions n ont aucun effet de bord : elles se testent sans base
de donnees ni Airflow.
"""
from datetime import datetime

from src.quality import (
    calculer_montant,
    imputer_prix,
    parse_date,
    quantite_valide,
    resoudre_client,
    to_date_key,
)


class TestParseDate:
    """La normalisation doit accepter les formats presents dans les
    sources et rejeter ce qui est illisible."""

    def test_format_nominal(self):
        assert parse_date("2026-08-01 16:30:48") == datetime(2026, 8, 1, 16, 30, 48)

    def test_format_date_seule(self):
        assert parse_date("2026-08-01") == datetime(2026, 8, 1, 0, 0, 0)

    def test_format_jour_mois_annee(self):
        """04/08/2026 doit etre lu comme le 4 aout, pas le 8 avril."""
        assert parse_date("04/08/2026") == datetime(2026, 8, 4)

    def test_date_illisible_rejetee(self):
        assert parse_date("date_invalide") is None

    def test_valeur_vide_rejetee(self):
        assert parse_date("") is None
        assert parse_date("   ") is None
        assert parse_date(None) is None

    def test_espaces_ignores(self):
        assert parse_date("  2026-08-01  ") == datetime(2026, 8, 1)


class TestDateKey:
    def test_conversion(self):
        assert to_date_key(datetime(2026, 8, 1, 14, 0)) == 20260801

    def test_mois_et_jour_sur_deux_chiffres(self):
        assert to_date_key(datetime(2026, 1, 5)) == 20260105


class TestCalculMontant:
    """Regle 4.6.6 : total_amount = quantity x unit_price."""

    def test_calcul_simple(self):
        assert calculer_montant(3, 165000) == 495000.0

    def test_accepte_les_chaines(self):
        assert calculer_montant("2", "450000") == 900000.0

    def test_arrondi_deux_decimales(self):
        assert calculer_montant(3, 10.005) == 30.02


class TestQuantiteValide:
    def test_quantite_positive_acceptee(self):
        assert quantite_valide("3") is True

    def test_quantite_negative_refusee(self):
        assert quantite_valide("-3") is False

    def test_quantite_nulle_refusee(self):
        assert quantite_valide("0") is False

    def test_quantite_non_numerique_refusee(self):
        assert quantite_valide("abc") is False
        assert quantite_valide(None) is False


class TestImputationPrix:
    CATALOGUE = {"P001": 450000, "P002": 520000}

    def test_prix_present_conserve(self):
        assert imputer_prix("32000", "P001", self.CATALOGUE) == 32000.0

    def test_prix_manquant_impute_du_catalogue(self):
        assert imputer_prix(None, "P001", self.CATALOGUE) == 450000.0

    def test_prix_illisible_impute_du_catalogue(self):
        assert imputer_prix("abc", "P002", self.CATALOGUE) == 520000.0

    def test_produit_absent_du_catalogue_irrecuperable(self):
        assert imputer_prix(None, "P999", self.CATALOGUE) is None


class TestResolutionClient:
    CONNUS = {"C001", "C002"}

    def test_client_connu_conserve(self):
        assert resoudre_client("C001", self.CONNUS) == "C001"

    def test_client_manquant_rattache_a_unknown(self):
        assert resoudre_client(None, self.CONNUS) == "UNKNOWN"

    def test_client_inconnu_rattache_a_unknown(self):
        assert resoudre_client("C999", self.CONNUS) == "UNKNOWN"

    def test_chaine_vide_rattachee_a_unknown(self):
        assert resoudre_client("", self.CONNUS) == "UNKNOWN"