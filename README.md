# TAISS_PROJECT — Retail Data Pipeline

[![Tests](https://github.com/barandzo/TAISSENG/actions/workflows/tests.yml/badge.svg)](https://github.com/barandzo/TAISSENG/actions/workflows/tests.yml)

Mini-plateforme Data Engineering : ingestion de fichiers de transactions
multi-magasins, contrôle qualité, chargement dans un Data Warehouse
dimensionnel PostgreSQL et exposition d'indicateurs via une API REST.

## Architecture

```
data/transactions/*.csv
        │
        ▼
   ┌─────────────────────── Apache Airflow ───────────────────────┐
   │ load_dimensions → ingest_data → validate_data                │
   │                       → transform_data → load_warehouse      │
   └──────────────────────────────┬───────────────────────────────┘
                                  ▼
                    PostgreSQL — modèle en étoile
                    fact_sales + 4 dimensions
                    + rejected_transactions + etl_file_log
                                  │
                                  ▼
                      FastAPI (conteneur Docker)
                      /sales/summary, /sales/by-store, ...
```

### Choix techniques

| Composant | Rôle | Justification |
|---|---|---|
| **Apache Airflow** | Orchestration | Dépendances explicites entre tâches, reprise sur erreur, historique des exécutions, logs par tâche |
| **PostgreSQL** | Data Warehouse | Contraintes d'intégrité natives, `ON CONFLICT` pour l'idempotence, `COPY` pour le chargement en masse, JSONB pour tracer les rejets |
| **FastAPI** | Exposition | Documentation OpenAPI générée automatiquement, validation des paramètres, performances asynchrones |
| **Docker Compose** | Reproductibilité | Environnement identique sur toutes les machines, un seul `up` pour tout démarrer |

## Modèle dimensionnel

Schéma en étoile, grain de la table de faits : **une ligne = une transaction**.

```
                         ┌──────────────┐
                         │   dim_date   │
                         │  date_key PK │
                         └──────┬───────┘
                                │
┌──────────────┐         ┌──────▼───────────┐         ┌──────────────┐
│ dim_product  │         │   fact_sales     │         │  dim_store   │
│ product_key  ├────────►│ transaction_id U │◄────────┤  store_key   │
│ product_id   │         │ date_key      FK │         │  store_id    │
│ product_name │         │ store_key     FK │         │  store_name  │
│ category     │         │ product_key   FK │         │  city        │
│ catalog_price│         │ customer_key  FK │         │  country     │
└──────────────┘         │ quantity      M  │         └──────────────┘
                         │ unit_price    M  │
                         │ total_amount  M  │
                         │ transaction_ts   │
                         │ source_file      │
                         └──────┬───────────┘
                                │
                         ┌──────▼───────────┐
                         │  dim_customer    │
                         │  customer_key    │
                         │  customer_id     │
                         │  customer_name   │
                         │  city            │
                         │  customer_type   │
                         └──────────────────┘

M = mesure   PK = clé primaire   FK = clé étrangère   U = contrainte unique
```

### Décisions de conception

- **Schéma en étoile plutôt que flocon** : moins de jointures, lecture
  analytique rapide, lisible par un utilisateur métier. La faible cardinalité
  de `category` et `city` ne justifie pas une normalisation supplémentaire.
- **Clés de substitution** (`product_key`, `store_key`…) : découplent le DWH
  des identifiants sources, préparent une future gestion SCD type 2, et les
  jointures sur entier sont plus rapides.
- **`transaction_id` en contrainte UNIQUE** sur `fact_sales` : garantit
  l'idempotence du pipeline.
- **Membres `UNKNOWN`** dans les dimensions : une vente dont le client est
  manquant reste comptabilisée dans le chiffre d'affaires plutôt que rejetée.
- **`dim_date` pré-générée** (2025-2027, 1095 lignes) : permet les analyses
  par mois, trimestre ou week-end sans calcul à la volée.
- **`total_amount` stocké** et non recalculé à la lecture : mesure additive
  pré-calculée, l'API se contente d'agréger.

## Règles de qualité

Principe directeur : **réparer** quand l'information est récupérable de façon
fiable, **rejeter** sinon. Tout rejet est tracé dans `rejected_transactions`
avec son motif et l'enregistrement brut en JSONB. Aucune donnée ne disparaît
silencieusement.

| Anomalie | Traitement | Justification |
|---|---|---|
| Doublon strict / `transaction_id` répété | Supprimé | Redondance d'export, pas une vente réelle |
| `customer_id` manquant | Réparé → `UNKNOWN` | La vente est réelle, le client n'est qu'un axe d'analyse |
| `unit_price` manquant | Réparé → prix catalogue | Information fiable disponible dans `products.csv` |
| Date au format `JJ/MM/AAAA` | Normalisée | Date métier présente, seule l'heure manque |
| Date illisible (`date_invalide`) | **Rejeté** | Sans date, aucun rattachement possible à `dim_date` |
| `quantity` ≤ 0 | **Rejeté** | Une vente de quantité négative fausserait le CA |
| `product_id` inexistant (`P999`) | **Rejeté** | Exigence explicite, prix invérifiable |
| `store_id` inexistant (`S999`) | **Rejeté** | Incohérence référentielle de la source |

## Prérequis

- Docker Desktop
- 4 Go de RAM disponibles

## Démarrage

```bash
cp .env.example .env          # puis renseigner les valeurs
docker compose up -d --build
```

Initialiser le schéma (une seule fois) :

```bash
docker cp sql/01_schema.sql taiss_postgres:/tmp/01_schema.sql
docker exec -it taiss_postgres psql -U taiss -d warehouse -f /tmp/01_schema.sql

docker cp sql/02_staging.sql taiss_postgres:/tmp/02_staging.sql
docker exec -it taiss_postgres psql -U taiss -d warehouse -f /tmp/02_staging.sql

docker cp sql/03_views.sql taiss_postgres:/tmp/03_views.sql
docker exec -it taiss_postgres psql -U taiss -d warehouse -f /tmp/03_views.sql
```

Lancer le pipeline :

```bash
docker exec -it taiss_airflow_scheduler airflow dags unpause retail_sales_pipeline
docker exec -it taiss_airflow_scheduler airflow dags trigger retail_sales_pipeline
```

## Accès

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| API (documentation) | http://localhost:8000/docs | — |
| PostgreSQL (depuis l'hôte) | `localhost:5433` | `taiss` / `taiss`, base `warehouse` |

Le port 5433 côté hôte est mappé sur le 5432 du conteneur, afin d'éviter tout
conflit avec une installation PostgreSQL locale. À l'intérieur du réseau
Docker, les services se joignent via `postgres:5432`.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | État de l'API et de la base |
| `GET /sales/summary` | CA, volume, panier moyen, meilleur produit |
| `GET /sales/by-store` | CA par point de vente |
| `GET /sales/by-product` | Top produits (paramètre `?limit=`) |
| `GET /sales/by-category` | Répartition du CA par catégorie |
| `GET /sales/daily` | Évolution quotidienne du CA |
| `GET /quality/report` | Rejets par motif, journal des fichiers traités |

Les endpoints de ventes acceptent un filtre de période :
`?date_from=AAAA-MM-JJ&date_to=AAAA-MM-JJ`.

Toutes les requêtes SQL sont paramétrées : aucune valeur fournie par
l'utilisateur n'est concaténée dans une requête.

## Pipeline Airflow

```
load_dimensions → ingest_data → validate_data → transform_data → load_warehouse
```

`load_dimensions` est placé en amont car la validation référentielle et la
résolution des clés de substitution supposent des dimensions à jour.

| Tâche | Rôle |
|---|---|
| `load_dimensions` | Upsert des dimensions depuis les fichiers de référence (`ON CONFLICT DO UPDATE`) |
| `ingest_data` | Détecte les fichiers non encore traités, les lit en texte brut, les concatène |
| `validate_data` | Applique les règles de qualité, écrit les rejets tracés en base |
| `transform_data` | Calcule `total_amount` et `date_key`, typage final |
| `load_warehouse` | `COPY` vers le staging, résolution des clés par jointure, insertion idempotente |

## Ajouter un nouveau fichier

Déposer un CSV dans `data/transactions/` et relancer le DAG. Aucune
modification de code n'est nécessaire : `ingest_data` compare le contenu du
dossier au journal `etl_file_log` et ne traite que les fichiers absents.

Générer un fichier de démonstration contenant des anomalies volontaires :

```bash
docker exec -it taiss_airflow_scheduler python /opt/airflow/src/make_demo_file.py
docker exec -it taiss_airflow_scheduler airflow dags trigger retail_sales_pipeline
```

## Idempotence

Relancer le DAG ne duplique aucune donnée, grâce à deux mécanismes
complémentaires :

1. **`etl_file_log`** : un fichier déjà chargé avec succès n'est pas relu.
2. **`ON CONFLICT (transaction_id) DO NOTHING`** : filet de sécurité au niveau
   de la base, même si une transaction franchissait la première barrière.

## Résultats obtenus

| Indicateur | Valeur |
|---|---|
| Lignes lues | 4 512 |
| Chargées dans `fact_sales` | 4 475 |
| Rejetées (tracées) | 25 |
| Doublons supprimés | 12 |
| Taux d'acceptation | 99,44 % |

Répartition des rejets :

| Motif | Nombre |
|---|---|
| `date_invalide` | 7 |
| `quantity_negative_ou_nulle` | 6 |
| `produit_inexistant` | 6 |
| `magasin_inexistant` | 6 |

Détail par fichier source :

| Fichier | Lues | Chargées | Rejetées |
|---|---|---|---|
| `sales_2026_08_01.csv` | 900 | 900 | 0 |
| `sales_2026_08_02.csv` | 912 | 900 | 0 (12 doublons) |
| `sales_2026_08_03.csv` | 900 | 900 | 0 |
| `sales_2026_08_04.csv` | 900 | 893 | 7 |
| `sales_2026_08_05.csv` | 900 | 882 | 18 |

Les anomalies ne sont pas réparties uniformément : elles se concentrent sur
les fichiers du 4 et du 5 août, les doublons sur celui du 2. Dans un contexte
réel, cette information permettrait de remonter à un problème d'export sur un
magasin ou un jour précis — c'est la raison d'être de `etl_file_log`.

## Vues analytiques

| Vue | Contenu |
|---|---|
| `v_sales_detail` | Table de faits dénormalisée avec toutes ses dimensions |
| `v_daily_store_sales` | Agrégat journalier par magasin (CA, volume, transactions) |

Ces vues évitent de réécrire les quatre jointures dans chaque requête et
servent de socle à l'API comme à un futur dashboard.

## Structure du projet

```
TAISS_PROJECT/
├── api/
│   ├── main.py              endpoints FastAPI
│   ├── db.py                pool de connexions SQLAlchemy
│   ├── requirements.txt
│   └── Dockerfile
├── dags/
│   └── retail_pipeline_dag.py
├── data/
│   ├── reference/           products, customers, stores
│   ├── transactions/        fichiers de ventes
│   └── staging/             fichiers intermédiaires (générés)
├── docker/airflow/          image Airflow personnalisée
├── docs/                    schémas d'architecture
├── sql/
│   ├── 00_create_databases.sql
│   ├── 01_schema.sql        modèle dimensionnel
│   ├── 02_staging.sql       table tampon
│   └── 03_views.sql         vues analytiques
├── src/
│   ├── config.py            chemins et DSN
│   ├── db.py                accès PostgreSQL
│   ├── ingest.py            détection et lecture des fichiers
│   ├── quality.py           règles de validation et nettoyage
│   ├── transform.py         calcul des mesures
│   ├── load.py              chargement dimensions et faits
│   ├── profile_data.py      profilage des anomalies
│   └── make_demo_file.py    générateur de fichier de démonstration
├── docker-compose.yml
├── .env.example
└── README.md
```

## Réinitialiser

Vider le Data Warehouse et rejouer l'ensemble des fichiers :

```bash
docker exec -it taiss_postgres psql -U taiss -d warehouse \
  -c "TRUNCATE fact_sales, rejected_transactions, etl_file_log RESTART IDENTITY;"
docker exec -it taiss_airflow_scheduler airflow dags trigger retail_sales_pipeline
```

Réinitialisation complète, métadonnées Airflow comprises :

```bash
docker compose down -v && docker compose up -d --build
```

Le schéma SQL doit alors être réappliqué (voir section Démarrage).

## Dépannage

| Symptôme | Cause probable | Correctif |
|---|---|---|
| `port is already allocated` | Port 5433, 8080 ou 8000 occupé | Modifier le port hôte dans `docker-compose.yml` |
| `warehouse` absent de `\l` | Volume Postgres initialisé avant l'ajout du script | `docker compose down -v` puis `up -d --build` |
| `airflow dags list` → `No data found` | Fichier absent de `dags/` ou erreur d'import | `airflow dags list-import-errors` |
| Modifications de `src/` non prises en compte | Modules Python en cache | `docker compose restart airflow-scheduler` |
| Webserver en boucle de redémarrage | Échec de `airflow-init` | `docker compose logs airflow-init` |