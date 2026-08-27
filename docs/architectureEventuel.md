# Architecture cible — Passage à l'échelle et MLOps

Document de conception. Scénario : passage de 5 à plusieurs centaines de
points de vente, plusieurs millions de transactions, plateforme devenue
critique pour l'entreprise.

---

## 1. Limites de l'architecture actuelle

L'architecture cible répond à des limites identifiées, pas à une liste de
technologies. Voici ce qui casse concrètement lorsque le volume augmente.

| Limite actuelle | Ce qui casse à l'échelle |
|---|---|
| Fichiers déposés dans un dossier local | 300 magasins ne peuvent pas écrire dans un volume Docker. Aucun mécanisme de transport, aucune reprise sur coupure réseau |
| `LocalExecutor`, un seul worker | Les tâches s'exécutent en série sur une seule machine. Le traitement dépasse la fenêtre nocturne |
| pandas charge tout en mémoire | 5 millions de lignes en `dtype=str` saturent la RAM. `pd.concat` duplique l'ensemble |
| `TRUNCATE stg_transactions` global | Deux exécutions concurrentes s'écrasent mutuellement — d'où le `max_active_runs=1` actuel |
| PostgreSQL mono-instance | Point de défaillance unique. `fact_sales` non partitionnée : les scans s'allongent linéairement |
| Une seule instance d'API | Point de défaillance unique. Tout redémarrage provoque une coupure de service |
| Aucune métrique exportée | Une dérive de qualité n'est visible qu'en interrogeant la base manuellement |
| Déploiement manuel | Aucun test automatisé avant la mise en production |

---

## 2. Schéma d'architecture cible

```
┌─── SOURCES ────────────────────────────────────────────────────────┐
│  300 magasins : caisses, SFTP, API REST, bases locales             │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─── INGESTION ──────────────────────────────────────────────────────┐
│  Apache NiFi (cluster)                                             │
│  ListSFTP → FetchSFTP → ValidateRecord → RouteOnAttribute          │
│       ├── conformes ──────► Object Storage (zone raw)              │
│       └── non conformes ──► zone quarantaine + alerte              │
│  Back-pressure, reprise sur coupure, provenance de chaque flux     │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─── ORCHESTRATION ──────────────────────────────────────────────────┐
│  Apache Airflow (CeleryExecutor / KubernetesExecutor)              │
│  Scheduler HA (2 réplicas) + N workers auto-scalés                 │
│  DAG partitionné par jour × magasin → parallélisme réel            │
│  Transformations lourdes déportées vers Spark ou dbt               │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─── STOCKAGE ───────────────────────────────────────────────────────┐
│  PostgreSQL primaire + réplicas lecture                            │
│  fact_sales partitionnée par mois (PARTITION BY RANGE)             │
│  Sauvegardes PITR (WAL archivés)                                   │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─── EXPOSITION ─────────────────────────────────────────────────────┐
│              Ingress / Load Balancer                               │
│         ┌────────────┼────────────┐                                │
│      API Pod 1   API Pod 2   API Pod 3    ← HPA (2→10 réplicas)    │
│         └────────────┼────────────┘                                │
│                 Cache Redis                                        │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─── PLATEFORME ─────────────────────────────────────────────────────┐
│  Kubernetes : ordonnancement, auto-scaling, self-healing           │
│  CI/CD GitHub Actions : lint → tests → build → scan → déploiement  │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─── OBSERVABILITÉ ──────────────────────────────────────────────────┐
│  Prometheus (métriques) → Grafana (dashboards) → Alertmanager      │
│  Loki (logs)  ·  OpenTelemetry (traces)                            │
└────────────────────────────────────────────────────────────────────┘
```

---

## 3. Rôle de chaque composant

### 3.1 Apache NiFi — ingestion et gestion des flux

**Problème résolu.** Dans l'architecture actuelle, un fichier doit
« magiquement » apparaître dans un dossier local. Avec 300 magasins, il faut
un système de transport fiable, traçable et résilient.

**Rôle précis.** NiFi tire les fichiers depuis les sources (SFTP, API REST,
bases locales), les valide structurellement, les route selon leur conformité
et les dépose dans le stockage objet. Il gère le *back-pressure* : si l'aval
est saturé, il ralentit l'amont au lieu de perdre des données. Il gère aussi
la reprise automatique après coupure réseau.

**Pourquoi NiFi plutôt qu'un script cron.** La *data provenance* trace chaque
fichier de bout en bout — origine, horodatage, transformations appliquées.
Sur 300 sources, quand un magasin affirme avoir envoyé ses ventes, c'est ce
qui permet de vérifier en trente secondes.

**Séparation NiFi / Airflow.** NiFi fait du *flow-based* : flux continu,
orienté événement, sans notion de dépendance entre étapes métier. Airflow
fait du *batch orchestré* : dépendances explicites, planification, reprise au
niveau de la tâche. NiFi amène la donnée jusqu'au lac ; Airflow orchestre sa
transformation en information exploitable.

### 3.2 Apache Airflow — orchestration à l'échelle

**Changement d'exécuteur.** Passage de `LocalExecutor` à `CeleryExecutor` ou
`KubernetesExecutor`. Avec ce dernier, chaque tâche devient un pod éphémère :
isolation des dépendances, ressources dimensionnées tâche par tâche, scaling
automatique selon la charge.

**Partitionnement du DAG.** Au lieu d'une tâche traitant l'ensemble des
fichiers, on génère dynamiquement une branche par jour × magasin. 300
fichiers deviennent 300 tâches parallélisables. Un magasin en échec
n'invalide plus le lot entier.

**Sortie de pandas.** Au-delà de quelques millions de lignes, les
transformations sont déportées vers Spark (calcul distribué) ou dbt
(transformations en SQL directement dans le warehouse). Airflow reste
l'orchestrateur ; il ne réalise plus le calcul lui-même.

**Correction nécessaire.** Le `TRUNCATE stg_transactions` global doit
disparaître au profit d'une table de staging par exécution
(`stg_transactions_{run_id}`) ou de tables temporaires. C'est la condition
pour lever la contrainte `max_active_runs=1` et autoriser le parallélisme.

### 3.3 PostgreSQL — stockage

**Partitionnement.** `fact_sales PARTITION BY RANGE (date_key)`, avec une
partition par mois. Une requête sur août ne scanne qu'une seule partition
(*partition pruning*), et purger l'historique devient un `DROP PARTITION`
instantané plutôt qu'un `DELETE` coûteux.

**Haute disponibilité.** Réplication en streaming : un primaire en écriture,
des réplicas en lecture. L'API interroge les réplicas, ce qui isole la charge
analytique de la charge d'écriture du pipeline. Patroni assure le basculement
automatique en cas de panne du primaire.

**Limite à reconnaître.** Au-delà d'environ 100 millions de lignes avec des
requêtes analytiques lourdes, PostgreSQL atteint ses limites. La suite
logique est un moteur orienté colonne : ClickHouse, DuckDB, BigQuery ou
Snowflake. Reconnaître la limite de son propre choix technique vaut mieux
que de le défendre à tort.

### 3.4 Kubernetes — déploiement et élasticité

| Fonction | Apport concret |
|---|---|
| **HPA** (Horizontal Pod Autoscaler) | Nombre de réplicas API ajusté selon le CPU ou les requêtes/seconde. Le pic à l'ouverture des magasins est absorbé automatiquement |
| **Self-healing** | Un pod qui échoue à son `livenessProbe` est redémarré sans intervention humaine |
| **Rolling update** | Nouvelle version déployée pod par pod, sans coupure de service |
| **Secrets** | Mots de passe et DSN sortis des images et du dépôt Git |
| **Resource limits** | Un job Airflow gourmand ne peut plus asphyxier les autres charges du cluster |

Le `readinessProbe` pointe sur `/health`, endpoint déjà implémenté dans
l'API actuelle : le code est prêt pour Kubernetes sans modification.

### 3.5 CI/CD — GitHub Actions

```
push → lint (ruff)
     → tests unitaires (pytest sur les règles de qualité)
     → tests d'intégration (docker compose up + DAG sur jeu réduit)
     → build des images + tag = SHA du commit
     → scan de vulnérabilités (Trivy)
     → déploiement staging (automatique)
     → déploiement production (validation manuelle)
```

**Argument central.** Les règles de `quality.py` sont testables
unitairement. Un test vérifiant qu'une quantité négative est bien rejetée
empêche une régression silencieuse — le pire scénario en data, puisque le
pipeline continue de s'exécuter en vert tout en corrompant le warehouse.

Exemple de test :

```python
def test_quantite_negative_rejetee():
    df = pd.DataFrame({"quantity": ["-2", "3"]})
    resultat = appliquer_regles(df)
    assert len(resultat.valides) == 1
    assert resultat.rejets[0].motif == "quantity_negative_ou_nulle"
```

### 3.6 Prometheus / Grafana — observabilité

Trois familles de métriques, à distinguer clairement.

| Famille | Exemples | Alerte type |
|---|---|---|
| **Technique** | Latence API, CPU/RAM des pods, connexions PostgreSQL | p95 > 2 s pendant 5 min |
| **Pipeline** | Durée du DAG, tâches en échec, fraîcheur des données | Aucun chargement depuis 26 h |
| **Qualité métier** | Taux de rejet, rejets par motif, magasins silencieux | Taux de rejet > 5 % |

**Point essentiel.** La métrique la plus importante n'est pas le CPU, c'est
le **taux de rejet par magasin**. Un magasin dont le taux passe de 0,5 % à
40 % signale un problème de caisse en amont. Prometheus collecte,
Alertmanager notifie, l'exploitant contacte le magasin — la donnée est
corrigée à la source, pas six mois plus tard dans un rapport erroné.

L'endpoint `/quality/report` déjà développé constitue la base de cet export :
il suffit d'ajouter `prometheus-fastapi-instrumentator` pour exposer un
endpoint `/metrics` scrapé par Prometheus.

Dashboards Grafana envisagés :

- **Exploitation** : état des DAG, durée des exécutions, fraîcheur par source
- **Qualité** : taux d'acceptation global, rejets par motif, classement des magasins par taux de rejet
- **Métier** : chiffre d'affaires par magasin et par jour, top produits, panier moyen

---

## 4. Volet MLOps

Une fois l'historique constitué, deux cas d'usage naturels.

| Cas d'usage | Modèle | Valeur métier |
|---|---|---|
| Prévision de ventes | Séries temporelles par produit × magasin | Optimisation des réapprovisionnements, réduction des ruptures |
| Détection d'anomalies | Isolation Forest sur les transactions | Détection de fraude, erreurs de caisse |

**Chaîne MLOps.**

- **MLflow** : suivi des expériences, registre de modèles versionnés
- **Airflow** : orchestration du réentraînement périodique
- **Détection de dérive** : comparaison de la distribution des données de production avec celle de l'entraînement
- **Déploiement** : le modèle est servi comme un service à côté de l'API, avec les mêmes garanties Kubernetes

**Lien avec l'existant.** Le modèle dimensionnel construit dans ce projet
*est* la couche de features. La vue `v_sales_detail` fournit directement les
variables d'entraînement. Un pipeline data propre est le prérequis du machine
learning, pas un projet parallèle.

---

## 5. Trajectoire de migration

L'architecture cible ne se déploie pas d'un bloc. Trajectoire incrémentale
proposée, chaque phase étant déclenchée par un seuil concret.

| Phase | Contenu | Déclencheur |
|---|---|---|
| **1** | Tests unitaires + CI, métriques Prometheus, partitionnement de `fact_sales` | Immédiat, faible coût |
| **2** | CeleryExecutor, DAG partitionné, réplica PostgreSQL en lecture | ~50 magasins |
| **3** | NiFi, stockage objet, Kubernetes, HPA | ~150 magasins |
| **4** | Moteur orienté colonne, chaîne MLOps | Millions de transactions par mois |

---

## 6. Questions anticipées

**« Pourquoi NiFi si Airflow sait déjà lire du SFTP ? »**
Airflow orchestre des tâches planifiées ; il n'est pas conçu pour du flux
continu depuis des centaines de sources hétérogènes avec back-pressure et
provenance. Un `SFTPSensor` déployé sur 300 sources saturerait le scheduler.

**« Kubernetes n'est-il pas surdimensionné ? »**
Pour la version actuelle, oui — Docker Compose est le choix approprié.
Kubernetes se justifie dès lors qu'il faut de la haute disponibilité, de
l'auto-scaling et des déploiements sans coupure, soit à partir de la phase 3.

**« Que faut-il monitorer en priorité ? »**
La fraîcheur des données et le taux de rejet par magasin. Un pipeline peut
être techniquement vert tout en chargeant des données fausses — c'est le
risque spécifique aux plateformes de données.

**« Comment garantir qu'aucune donnée n'est perdue ? »**
Trois niveaux de garantie : NiFi assure le transport avec reprise sur
coupure, `etl_file_log` garantit qu'aucun fichier n'est oublié ni traité deux
fois, `rejected_transactions` garantit qu'aucune ligne écartée ne disparaît
sans trace ni motif.

**« Pourquoi ne pas tout faire en SQL / en Spark dès maintenant ? »**
Le volume actuel ne le justifie pas. Introduire Spark pour 4 500 lignes
ajouterait de la complexité opérationnelle sans gain. Le choix technique doit
suivre le volume, pas le précéder.