"""
Profilage des données brutes.
Objectif : mesurer les anomalies AVANT de définir les règles de nettoyage.
Ce script ne modifie rien, il lit et compte.
"""
import os
import glob
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
REF_DIR = os.path.join(DATA_DIR, "reference")
TX_DIR = os.path.join(DATA_DIR, "transactions")

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)


def section(titre):
    print("\n" + "=" * 70)
    print(titre)
    print("=" * 70)


# ---------------------------------------------------------------- références
section("1. FICHIERS DE REFERENCE")

products = pd.read_csv(os.path.join(REF_DIR, "products.csv"), encoding="utf-8")
customers = pd.read_csv(os.path.join(REF_DIR, "customers.csv"), encoding="utf-8")
stores = pd.read_csv(os.path.join(REF_DIR, "stores.csv"), encoding="utf-8")

for nom, df in [("products", products), ("customers", customers), ("stores", stores)]:
    print(f"\n--- {nom}.csv : {len(df)} lignes ---")
    print("Colonnes :", list(df.columns))
    print("Valeurs nulles :\n", df.isna().sum().to_string())
    cle = df.columns[0]
    print(f"Doublons sur {cle} :", int(df[cle].duplicated().sum()))

print("\nExemple products (verif encodage UTF-8) :")
print(customers.head(3).to_string(index=False))

# ------------------------------------------------------------- transactions
section("2. FICHIERS DE TRANSACTIONS")

fichiers = sorted(glob.glob(os.path.join(TX_DIR, "*.csv")))
print(f"{len(fichiers)} fichier(s) detecte(s) automatiquement :")
for f in fichiers:
    print("  -", os.path.basename(f))

frames = []
for f in fichiers:
    d = pd.read_csv(f, encoding="utf-8", dtype=str)  # tout en texte : on voit les formats bruts
    d["source_file"] = os.path.basename(f)
    frames.append(d)
    print(f"\n{os.path.basename(f)} : {len(d)} lignes")

tx = pd.concat(frames, ignore_index=True)
print(f"\nTOTAL brut : {len(tx)} lignes")

# ------------------------------------------------------------------ nullité
section("3. VALEURS MANQUANTES (toutes transactions)")
nulls = tx.isna().sum()
vides = (tx == "").sum()
print(pd.DataFrame({"NaN": nulls, "chaine_vide": vides}).to_string())

# ---------------------------------------------------------------- doublons
section("4. DOUBLONS")
cols = ["transaction_id", "store_id", "customer_id", "product_id",
        "quantity", "unit_price", "transaction_date"]
print("Lignes strictement identiques      :", int(tx.duplicated(subset=cols).sum()))
print("transaction_id apparaissant >1 fois:", int(tx["transaction_id"].duplicated().sum()))

dup_ids = tx[tx["transaction_id"].duplicated(keep=False)].sort_values("transaction_id")
if len(dup_ids):
    print("\nExemples de transaction_id dupliques :")
    print(dup_ids.head(10).to_string(index=False))

# ------------------------------------------------------------------- dates
section("5. FORMATS DE DATE")
parsee = pd.to_datetime(tx["transaction_date"], errors="coerce", format="mixed")
echecs = tx[parsee.isna() & tx["transaction_date"].notna()]
print("Dates non parsables :", len(echecs))
if len(echecs):
    print("Valeurs distinctes en echec :")
    print(echecs["transaction_date"].value_counts().head(20).to_string())

# longueurs de chaîne = indice de formats différents
print("\nRepartition des longueurs de la chaine date :")
print(tx["transaction_date"].dropna().str.len().value_counts().to_string())

# ------------------------------------------------------ intégrité référentielle
section("6. INTEGRITE REFERENTIELLE")
for col, ref, refcol in [("product_id", products, "product_id"),
                         ("store_id", stores, "store_id"),
                         ("customer_id", customers, "customer_id")]:
    connus = set(ref[refcol])
    inconnus = tx[~tx[col].isin(connus) & tx[col].notna()]
    print(f"\n{col} inconnus : {len(inconnus)} ligne(s)")
    if len(inconnus):
        print(inconnus[col].value_counts().head(10).to_string())

# ------------------------------------------------------------- valeurs numériques
section("7. VALEURS NUMERIQUES")
q = pd.to_numeric(tx["quantity"], errors="coerce")
p = pd.to_numeric(tx["unit_price"], errors="coerce")

print("quantity non numerique :", int(q.isna().sum() - tx["quantity"].isna().sum()))
print("quantity <= 0          :", int((q <= 0).sum()))
print("quantity stats         :", q.min(), "->", q.max())
print("unit_price non numerique:", int(p.isna().sum() - tx["unit_price"].isna().sum()))
print("unit_price <= 0        :", int((p <= 0).sum()))
print("unit_price stats       :", p.min(), "->", p.max())

# écart entre prix transaction et prix catalogue
section("8. COHERENCE PRIX TRANSACTION vs CATALOGUE")
m = tx.assign(px=p).merge(
    products[["product_id", "unit_price"]].rename(columns={"unit_price": "px_catalogue"}),
    on="product_id", how="left")
ecart = m[m["px_catalogue"].notna() & (m["px"] != m["px_catalogue"])]
print("Lignes avec prix != catalogue :", len(ecart))
if len(ecart):
    print(ecart[["transaction_id", "product_id", "px", "px_catalogue"]].head(10).to_string(index=False))

print("\n" + "=" * 70)
print("FIN DU PROFILAGE")
print("=" * 70)