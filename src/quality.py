"""Etape 2 - Validation et nettoyage des transactions.

Organisation du module en trois couches :

  1. REGLES PURES        fonctions sans effet de bord, testables isolement
  2. ORCHESTRATION PURE  applique les regles a un DataFrame et retourne
                         un resultat ; ne lit ni n ecrit rien
  3. EFFETS DE BORD      lecture de fichier, ecriture en base, XCom Airflow

Cette separation permet de tester la logique metier sans PostgreSQL ni
Airflow. C est essentiel : une regle de qualite erronee ne fait pas
planter le pipeline, elle charge silencieusement des donnees fausses.
"""
import os
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

from src.config import REF_DIR, STAGING_DIR, RAW_COLUMNS
from src.db import get_conn

log = logging.getLogger(__name__)


# =====================================================================
# 1. REGLES PURES
# =====================================================================

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


def parse_date(valeur) -> Optional[datetime]:
    """Normalise une date vers un datetime.

    Aucun format americain (MM/JJ/AAAA) n est teste : les donnees sources
    utilisent le format europeen, et accepter les deux creerait une
    ambiguite indetectable sur des valeurs comme 04/08/2026.

    Retourne None si la valeur est illisible.
    """
    if valeur is None:
        return None
    if isinstance(valeur, float) and pd.isna(valeur):
        return None
    texte = str(valeur).strip()
    if not texte or texte.lower() in ("nan", "none"):
        return None
    for fmt in FORMATS_DATE:
        try:
            return datetime.strptime(texte, fmt)
        except ValueError:
            continue
    return None


def to_date_key(moment: datetime) -> int:
    """Convertit un datetime en cle de dimension date au format AAAAMMJJ."""
    return int(moment.strftime("%Y%m%d"))


def calculer_montant(quantite, prix_unitaire) -> float:
    """Calcule le montant total d une transaction (regle 4.6.6)."""
    return round(float(quantite) * float(prix_unitaire), 2)


def quantite_valide(valeur) -> bool:
    """Une quantite doit etre numerique et strictement positive.

    Une quantite negative ou nulle est incoherente pour une vente et
    fausserait le chiffre d affaires : la ligne est rejetee, pas reparee.
    """
    nombre = pd.to_numeric(valeur, errors="coerce")
    if pd.isna(nombre):
        return False
    return bool(nombre > 0)


def imputer_prix(prix_source, product_id, prix_catalogue: dict) -> Optional[float]:
    """Retourne le prix a utiliser pour une transaction.

    Si le prix source est absent ou illisible, on impute le prix catalogue
    du produit : l information est recuperable de facon fiable, donc on
    repare plutot que de rejeter. Retourne None si irrecuperable.
    """
    nombre = pd.to_numeric(prix_source, errors="coerce")
    if not pd.isna(nombre):
        return float(nombre)
    catalogue = prix_catalogue.get(product_id)
    return float(catalogue) if catalogue is not None else None


def resoudre_client(customer_id, ids_connus: set) -> str:
    """Rattache une transaction a un client connu ou au membre UNKNOWN.

    Une vente dont le client est manquant reste une vente reelle : la
    perdre fausserait le chiffre d affaires. Le client n est qu un axe
    d analyse, on la rattache donc au membre inconnu de la dimension.
    """
    if customer_id is None:
        return "UNKNOWN"
    if isinstance(customer_id, float) and pd.isna(customer_id):
        return "UNKNOWN"
    texte = str(customer_id).strip()
    if texte == "" or texte.lower() in ("nan", "none"):
        return "UNKNOWN"
    if texte not in ids_connus:
        return "UNKNOWN"
    return texte


# =====================================================================
# 2. ORCHESTRATION PURE
# =====================================================================

@dataclass
class Referentiel:
    """Donnees de reference necessaires a la validation."""
    ids_produits: set
    ids_magasins: set
    ids_clients: set
    prix_catalogue: dict

    @classmethod
    def depuis_csv(cls, ref_dir: str) -> "Referentiel":
        produits = pd.read_csv(os.path.join(ref_dir, "products.csv"), encoding="utf-8")
        magasins = pd.read_csv(os.path.join(ref_dir, "stores.csv"), encoding="utf-8")
        clients = pd.read_csv(os.path.join(ref_dir, "customers.csv"), encoding="utf-8")
        return cls(
            ids_produits=set(produits["product_id"]),
            ids_magasins=set(magasins["store_id"]),
            ids_clients=set(clients["customer_id"]),
            prix_catalogue=dict(zip(produits["product_id"], produits["unit_price"])),
        )


@dataclass
class ResultatValidation:
    """Sortie de la validation : lignes retenues, rejets et compteurs."""
    valides: pd.DataFrame
    rejets: list = field(default_factory=list)   # [(DataFrame, motif), ...]
    doublons_supprimes: int = 0
    prix_imputes: int = 0
    clients_inconnus: int = 0

    @property
    def nb_rejets(self) -> int:
        return sum(len(bloc) for bloc, _ in self.rejets)

    def motifs(self) -> dict:
        """Compte des rejets par motif."""
        compteur = Counter()
        for bloc, motif in self.rejets:
            compteur[motif] += len(bloc)
        return dict(compteur)


def _colonne(serie, fonction):
    """Applique une fonction a une colonne en preservant le type Series.

    Sur une Series vide, pandas.Series.apply retourne un DataFrame sans
    colonnes au lieu d une Series. Le masquage booleen qui suit detruirait
    alors toutes les colonnes du DataFrame. On force donc une Series.
    """
    if len(serie) == 0:
        return pd.Series(dtype=object, index=serie.index)
    return serie.apply(fonction)


def _masque(serie, fonction) -> pd.Series:
    """Variante de _colonne retournant une Series booleenne."""
    if len(serie) == 0:
        return pd.Series(dtype=bool, index=serie.index)
    return serie.apply(fonction).astype(bool)


def _ecarter(df, masque, motif, rejets):
    """Retire du DataFrame les lignes correspondant au masque."""
    if len(df) == 0:
        return df
    mauvaises = df[masque]
    if len(mauvaises):
        rejets.append((mauvaises.copy(), motif))
        log.warning("  REJET %-32s : %d ligne(s)", motif, len(mauvaises))
    return df[~masque].copy()


def appliquer_regles(df: pd.DataFrame, ref: Referentiel) -> ResultatValidation:
    """Applique l ensemble des regles de qualite a un DataFrame brut.

    Fonction PURE : ne lit aucun fichier, n ecrit dans aucune base.
    C est le coeur testable du module.

    Principe : reparer quand l information est recuperable de facon
    fiable, rejeter sinon. Tout rejet est conserve dans le resultat.
    """
    df = df.copy()
    rejets = []

    # R1 - Doublons : redondance d export, pas une anomalie de donnee.
    # Supprimes sans etre traces comme rejets.
    avant = len(df)
    df = df.drop_duplicates(subset=RAW_COLUMNS, keep="first")
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    doublons = avant - len(df)
    if doublons:
        log.info("  R1 doublons supprimes            : %d", doublons)

    # R2 - Identifiant de transaction obligatoire
    df = _ecarter(
        df,
        df["transaction_id"].isna() | (df["transaction_id"].astype(str).str.strip() == ""),
        "transaction_id_manquant",
        rejets,
    )

    # R3 - Date exploitable. Non reparable si illisible : sans date, la
    # ligne n est rattachable a aucune entree de dim_date.
    df["transaction_ts"] = _colonne(df["transaction_date"], parse_date)
    df = _ecarter(df, df["transaction_ts"].isna(), "date_invalide", rejets)

    # R4 - Quantite strictement positive
    df = _ecarter(df, ~_masque(df["quantity"], quantite_valide),
                  "quantity_negative_ou_nulle", rejets)
    df["quantity_num"] = pd.to_numeric(df["quantity"])

    # R5 - Integrite referentielle
    df = _ecarter(df, ~df["product_id"].isin(ref.ids_produits),
                  "produit_inexistant", rejets)
    df = _ecarter(df, ~df["store_id"].isin(ref.ids_magasins),
                  "magasin_inexistant", rejets)

    # R6 - Prix unitaire : imputation depuis le catalogue si manquant
    manquants = int(pd.to_numeric(df["unit_price"], errors="coerce").isna().sum())
    if len(df):
        df["unit_price_num"] = df.apply(
            lambda r: imputer_prix(r["unit_price"], r["product_id"], ref.prix_catalogue),
            axis=1,
        )
    else:
        df["unit_price_num"] = pd.Series(dtype="float64")
    df = _ecarter(df, df["unit_price_num"].isna(), "prix_irrecuperable", rejets)
    df = _ecarter(df, df["unit_price_num"] < 0, "prix_negatif", rejets)
    prix_imputes = max(manquants - sum(
        len(b) for b, m in rejets if m in ("prix_irrecuperable", "prix_negatif")
    ), 0)
    if prix_imputes:
        log.info("  R6 prix imputes du catalogue     : %d", prix_imputes)

    # R7 - Client manquant ou inconnu rattache au membre UNKNOWN
    if len(df):
        avant_clients = df["customer_id"].astype(str)
        df["customer_id"] = _colonne(
            df["customer_id"], lambda c: resoudre_client(c, ref.ids_clients)
        )
        clients_inconnus = int((df["customer_id"] != avant_clients).sum())
    else:
        clients_inconnus = 0
    if clients_inconnus:
        log.info("  R7 clients rattaches a UNKNOWN   : %d", clients_inconnus)

    return ResultatValidation(
        valides=df,
        rejets=rejets,
        doublons_supprimes=doublons,
        prix_imputes=prix_imputes,
        clients_inconnus=clients_inconnus,
    )


# =====================================================================
# 3. EFFETS DE BORD
# =====================================================================

def validate_data(**context):
    """Tache Airflow : lit le fichier ingere, applique les regles,
    ecrit les rejets en base et transmet le fichier nettoye."""
    ti = context["ti"]
    ts = context["ts_nodash"]
    chemin_raw = ti.xcom_pull(task_ids="ingest_data", key="raw_path")

    if not chemin_raw:
        ti.xcom_push(key="clean_path", value="")
        ti.xcom_push(key="rejected_by_file", value={})
        return 0

    df = pd.read_csv(chemin_raw, encoding="utf-8", dtype=str)
    log.info("Lignes en entree : %d", len(df))

    ref = Referentiel.depuis_csv(REF_DIR)
    resultat = appliquer_regles(df, ref)

    rejets_par_fichier = _ecrire_rejets(resultat.rejets)

    chemin_clean = os.path.join(STAGING_DIR, "clean_" + ts + ".csv")
    resultat.valides.to_csv(chemin_clean, index=False, encoding="utf-8")

    log.info("BILAN : %d lues | %d valides | %d rejetees | %d doublons",
             len(df), len(resultat.valides), resultat.nb_rejets,
             resultat.doublons_supprimes)

    ti.xcom_push(key="clean_path", value=chemin_clean)
    ti.xcom_push(key="rows_rejected", value=resultat.nb_rejets)
    ti.xcom_push(key="rejected_by_file", value=rejets_par_fichier)
    return len(resultat.valides)


def _ecrire_rejets(rejets) -> dict:
    """Persiste les rejets et retourne leur compte par fichier source."""
    if not rejets:
        return {}

    cols = RAW_COLUMNS + ["source_file"]
    lignes = []
    compteur = Counter()
    for bloc, motif in rejets:
        brut = bloc[cols].where(pd.notna(bloc[cols]), None)
        for enreg in brut.to_dict(orient="records"):
            compteur[enreg.get("source_file")] += 1
            lignes.append((
                enreg.get("transaction_id"),
                enreg.get("source_file"),
                motif,
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