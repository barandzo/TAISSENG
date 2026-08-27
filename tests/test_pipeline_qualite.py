"""Tests de l orchestration des regles sur un DataFrame complet.

Verifie que les lignes valides passent et que chaque anomalie est
traitee selon la strategie retenue : reparation ou rejet motive.
"""
from src.quality import appliquer_regles
from tests.conftest import ligne


class TestLignesValides:
    """Un pipeline qui rejette tout serait sur mais inutile."""

    def test_ligne_conforme_conservee(self, make_df, referentiel):
        df = make_df(ligne())
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 1
        assert res.nb_rejets == 0

    def test_plusieurs_lignes_conformes(self, make_df, referentiel):
        df = make_df(
            ligne(transaction_id="T1"),
            ligne(transaction_id="T2", product_id="P002"),
            ligne(transaction_id="T3", store_id="S002"),
        )
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 3
        assert res.nb_rejets == 0


class TestRejets:
    """Chaque anomalie non reparable doit produire un rejet motive."""

    def test_quantite_negative_rejetee(self, make_df, referentiel):
        df = make_df(ligne(quantity="-2"))
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 0
        assert res.motifs() == {"quantity_negative_ou_nulle": 1}

    def test_produit_inexistant_rejete(self, make_df, referentiel):
        df = make_df(ligne(product_id="P999"))
        res = appliquer_regles(df, referentiel)
        assert res.motifs() == {"produit_inexistant": 1}

    def test_magasin_inexistant_rejete(self, make_df, referentiel):
        df = make_df(ligne(store_id="S999"))
        res = appliquer_regles(df, referentiel)
        assert res.motifs() == {"magasin_inexistant": 1}

    def test_date_illisible_rejetee(self, make_df, referentiel):
        df = make_df(ligne(transaction_date="date_invalide"))
        res = appliquer_regles(df, referentiel)
        assert res.motifs() == {"date_invalide": 1}

    def test_rejet_conserve_la_ligne_brute(self, make_df, referentiel):
        """Aucune donnee ne doit disparaitre silencieusement."""
        df = make_df(ligne(transaction_id="T042", quantity="-5"))
        res = appliquer_regles(df, referentiel)
        bloc, motif = res.rejets[0]
        assert bloc.iloc[0]["transaction_id"] == "T042"
        assert bloc.iloc[0]["quantity"] == "-5"


class TestReparations:
    """Quand l information est recuperable, on repare au lieu de rejeter."""

    def test_prix_manquant_impute(self, make_df, referentiel):
        df = make_df(ligne(product_id="P002", unit_price=None))
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 1
        assert res.valides.iloc[0]["unit_price_num"] == 520000.0

    def test_client_manquant_rattache_a_unknown(self, make_df, referentiel):
        df = make_df(ligne(customer_id=None))
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 1
        assert res.valides.iloc[0]["customer_id"] == "UNKNOWN"

    def test_date_format_court_normalisee(self, make_df, referentiel):
        df = make_df(ligne(transaction_date="04/08/2026"))
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 1
        assert res.valides.iloc[0]["transaction_ts"].day == 4
        assert res.valides.iloc[0]["transaction_ts"].month == 8


class TestDoublons:
    """Un doublon est une redondance technique, pas une anomalie :
    il est supprime et non compte comme rejet."""

    def test_doublon_strict_supprime(self, make_df, referentiel):
        df = make_df(ligne(), ligne())
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 1
        assert res.doublons_supprimes == 1
        assert res.nb_rejets == 0

    def test_transaction_id_duplique_supprime(self, make_df, referentiel):
        """Meme id mais quantites differentes : on garde la premiere."""
        df = make_df(
            ligne(transaction_id="T1", quantity="2"),
            ligne(transaction_id="T1", quantity="3"),
        )
        res = appliquer_regles(df, referentiel)
        assert len(res.valides) == 1
        assert res.valides.iloc[0]["quantity"] == "2"


class TestScenarioComplet:
    """Reproduit en miniature la composition du jeu de donnees du TP."""

    def test_lot_mixte(self, make_df, referentiel):
        df = make_df(
            ligne(transaction_id="T1"),                                   # valide
            ligne(transaction_id="T2", product_id="P002"),                # valide
            ligne(transaction_id="T3", quantity="-1"),                    # rejet
            ligne(transaction_id="T4", product_id="P999"),                # rejet
            ligne(transaction_id="T5", store_id="S999"),                  # rejet
            ligne(transaction_id="T6", transaction_date="date_invalide"), # rejet
            ligne(transaction_id="T7", customer_id=None),                 # repare
            ligne(transaction_id="T8", unit_price=None),                  # repare
            ligne(transaction_id="T1"),                                   # doublon
        )
        res = appliquer_regles(df, referentiel)

        assert len(res.valides) == 4        # T1, T2, T7, T8
        assert res.nb_rejets == 4           # T3, T4, T5, T6
        assert res.doublons_supprimes == 1

        # Controle d integrite : aucune ligne ne se perd
        assert len(res.valides) + res.nb_rejets + res.doublons_supprimes == len(df)