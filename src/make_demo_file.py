"""Genere un fichier de transactions pour la demonstration.

Reprend la structure des fichiers sources avec des transaction_id neufs
et quelques anomalies volontaires, afin de montrer en direct la detection
automatique et le traitement qualite.
"""
import os
import random
from datetime import datetime, timedelta

import pandas as pd

from src.config import REF_DIR, TX_DIR

random.seed(42)

produits = pd.read_csv(os.path.join(REF_DIR, "products.csv"), encoding="utf-8")
clients = pd.read_csv(os.path.join(REF_DIR, "customers.csv"), encoding="utf-8")
magasins = pd.read_csv(os.path.join(REF_DIR, "stores.csv"), encoding="utf-8")

JOUR = "2026-08-06"
NB = 300
lignes = []

for i in range(NB):
    prod = produits.sample(1).iloc[0]
    heure = datetime.strptime(JOUR, "%Y-%m-%d") + timedelta(
        hours=random.randint(8, 19), minutes=random.randint(0, 59)
    )
    lignes.append({
        "transaction_id": "T90%04d" % i,
        "store_id": magasins.sample(1).iloc[0]["store_id"],
        "customer_id": clients.sample(1).iloc[0]["customer_id"],
        "product_id": prod["product_id"],
        "quantity": random.randint(1, 4),
        "unit_price": prod["unit_price"],
        "transaction_date": heure.strftime("%Y-%m-%d %H:%M:%S"),
    })

df = pd.DataFrame(lignes)

# Anomalies volontaires pour la demonstration
df.loc[0, "quantity"] = -2                      # quantite negative
df.loc[1, "product_id"] = "P999"                # produit inexistant
df.loc[2, "store_id"] = "S999"                  # magasin inexistant
df.loc[3, "transaction_date"] = "date_invalide" # date illisible
df.loc[4, "customer_id"] = None                 # client manquant -> UNKNOWN
df.loc[5, "unit_price"] = None                  # prix manquant -> catalogue
df = pd.concat([df, df.iloc[[10]]], ignore_index=True)  # doublon strict

chemin = os.path.join(TX_DIR, "sales_2026_08_06.csv")
df.to_csv(chemin, index=False, encoding="utf-8")

print("Fichier genere : %s" % chemin)
print("  lignes         : %d" % len(df))
print("  attendu charge : %d" % (len(df) - 5))
print("  attendu rejete : 4 (quantite, produit, magasin, date)")
print("  attendu repare : 2 (client UNKNOWN, prix catalogue)")
print("  doublon        : 1 supprime")