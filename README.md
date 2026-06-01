# Data Lake Vélo Lyon

Pipeline de données temps réel pour analyser les stations Vélo'v de Lyon. Le projet ingère les données de l'API JCDecaux, les stocke dans HDFS, les transforme avec MapReduce et les rend interrogeables via Hive.

## Contexte

Vélo Lyon (250 stations, 2500 vélos) fait face à un problème de répartition des vélos : stations vides en centre-ville aux heures de pointe, vélos bloqués en périphérie le soir. Ce data lake a pour but de fournir des métriques exploitables à trois profils :

- **Exploitation** : savoir où envoyer les camions de redistribution
- **Maintenance** : détecter les stations en panne avant les plaintes
- **Direction** : identifier les zones prioritaires pour investir

## Architecture

```
API JCDecaux (JSON, toutes les 60s)
      |
      v
Kafka Producer (producer.py)
      |
      v
Kafka (topic: velo_lyon_raw)
      |
      v
Kafka Consumer (consumer.py) --> kafka_collected.json
      |
      v
HDFS /data-lake/raw/velo_lyon/
      |
      v
4 Jobs MapReduce Python
  - MR1 : load_factor (taux de remplissage par station)
  - MR2 : anomalies (stations zombies, capteurs HS)
  - MR3 : agrégats horaire/quartier (pics de demande)
  - MR4 : heatmap stratégique (CA potentiel par quartier)
      |
      v
HDFS /data-lake/processed/
      |
      v
Hive (tables externes) --> 6 requêtes métier SQL
      |
      v
Atlas (gouvernance, lineage, RGPD)
```

## Prérequis

- Docker Desktop (avec au moins 8 Go de RAM allouée)
- Python 3.x avec pip
- Git

## Installation

Cloner le repo et lancer le cluster :

```bash
git clone https://github.com/VOTRE-USERNAME/tp-velo-lyon.git
cd tp-velo-lyon
docker compose up -d
```

Le premier lancement télécharge les images Docker, ça prend quelques minutes. Vérifier que tout tourne :

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Les 8 services doivent être "Up". Les 4 services Hadoop (namenode, datanode, resourcemanager, nodemanager) doivent afficher "healthy".

### Installation de Python dans les containers

Les images Hadoop n'incluent pas Python. Il faut l'installer manuellement après chaque `docker compose down` :

```bash
docker exec -it namenode bash -c "
sed -i 's|deb.debian.org|archive.debian.org|g' /etc/apt/sources.list && \
sed -i 's|security.debian.org|archive.debian.org|g' /etc/apt/sources.list && \
sed -i '/stretch-updates/d' /etc/apt/sources.list && \
apt-get update && apt-get install -y python3
"
```

Même commande pour le nodemanager (remplacer `namenode` par `nodemanager`).

### Installation des dépendances Python (sur la machine hôte)

```bash
python3 -m venv venv
source venv/bin/activate
pip install kafka-python requests
```

## Utilisation

### 1. Créer l'arborescence HDFS

```bash
docker exec -it namenode bash
hdfs dfs -mkdir -p /data-lake/raw/velo_lyon /data-lake/processed /data-lake/analytics
```

### 2. Collecter les données avec Kafka

Lancer le producer (tourne en boucle, Ctrl+C pour arrêter) :

```bash
python kafka/producer.py
```

Le producer appelle l'API JCDecaux toutes les 60 secondes et envoie les données dans le topic Kafka `velo_lyon_raw`. Laisser tourner quelques minutes pour accumuler des données.

Puis récupérer les données avec le consumer :

```bash
python kafka/consumer.py
```

Le consumer lit tous les messages du topic et les écrit dans `data/kafka_collected.json`.

### 3. Charger les données dans HDFS

Depuis le container namenode :

```bash
docker exec -it namenode bash
cd /tmp
PYTHONIOENCODING=utf-8 python3 -c "
import json, sys
with open('/opt/data/kafka_collected.json', encoding='utf-8') as f:
    for line in f:
        print(line.strip())
" > /tmp/data.json
hdfs dfs -put /tmp/data.json /data-lake/raw/velo_lyon/
```

### 4. Lancer les jobs MapReduce

Toujours depuis le container namenode, dans `/tmp` :

```bash
cd /tmp

# MR1 - Load Factor
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -input /data-lake/raw/velo_lyon/kafka_collected.json \
  -output /data-lake/processed/load_metrics \
  -mapper "python3 mapper_load_factor.py" \
  -reducer "python3 reducer_load_factor.py" \
  -file /opt/mapreduce/mapper_load_factor.py \
  -file /opt/mapreduce/reducer_load_factor.py

# MR2 - Anomalies
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -input /data-lake/raw/velo_lyon/kafka_collected.json \
  -output /data-lake/processed/anomalies \
  -mapper "python3 mapper_anomalies.py" \
  -reducer "python3 reducer_anomalies.py" \
  -file /opt/mapreduce/mapper_anomalies.py \
  -file /opt/mapreduce/reducer_anomalies.py

# MR3 - Agrégats horaire/quartier
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D stream.num.map.output.key.fields=2 \
  -input /data-lake/raw/velo_lyon/kafka_collected.json \
  -output /data-lake/processed/horaire_quartier \
  -mapper "python3 mapper_horaire.py" \
  -reducer "python3 reducer_horaire.py" \
  -file /opt/mapreduce/mapper_horaire.py \
  -file /opt/mapreduce/reducer_horaire.py

# MR4 - Heatmap stratégique
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D stream.num.map.output.key.fields=1 \
  -input /data-lake/raw/velo_lyon/kafka_collected.json \
  -output /data-lake/processed/heatmap \
  -mapper "python3 mapper_heatmap.py" \
  -reducer "python3 reducer_heatmap.py" \
  -file /opt/mapreduce/mapper_heatmap.py \
  -file /opt/mapreduce/reducer_heatmap.py
```

Vérifier les résultats :

```bash
hdfs dfs -cat /data-lake/processed/load_metrics/part-00000 | head -5
```

### 5. Requêtes Hive

Se connecter à Hive :

```bash
docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10000" -n root
```

Les tables et requêtes sont dans `hive/create_hive_tables.sql` et `hive/requetes_metier.sql`.

## Structure du projet

```
tp-velo-lyon/
├── docker-compose.yml           # Cluster Hadoop + Kafka + Hive + Atlas
├── config/
│   ├── core-site.xml            # Config Hadoop (localisation du namenode)
│   ├── hdfs-site.xml            # Config HDFS (réplication, stockage)
│   ├── mapred-site.xml          # Config MapReduce
│   └── yarn-site.xml            # Config YARN (ressources)
├── kafka/
│   ├── producer.py              # Collecte API → Kafka
│   └── consumer.py              # Kafka → fichier JSON
├── mapreduce/
│   ├── mapper_load_factor.py    # MR1 mapper
│   ├── reducer_load_factor.py   # MR1 reducer
│   ├── mapper_anomalies.py      # MR2 mapper
│   ├── reducer_anomalies.py     # MR2 reducer
│   ├── mapper_horaire.py        # MR3 mapper
│   ├── reducer_horaire.py       # MR3 reducer
│   ├── mapper_heatmap.py        # MR4 mapper
│   └── reducer_heatmap.py       # MR4 reducer
├── hive/
│   ├── create_hive_tables.sql   # Création des 5 tables externes
│   └── requetes_metier.sql      # 6 requêtes métier avec résultats
├── atlas/
│   └── atlas_entities.json      # Déclaration des entités Atlas
├── data/
│   └── sample.json              # Échantillon de données API
├── docs/
│   ├── architecture.png         # Diagramme du pipeline
│   └── lineage_screenshot.png   # Capture lineage Atlas
├── hdfs_inventory.txt           # Inventaire HDFS (hdfs dfs -ls -R)
├── .gitignore
└── .env                         # Clé API JCDecaux (non versionné)
```

## Jobs MapReduce

### MR1 — Load Factor

Calcule le taux de remplissage de chaque station. Le load_factor va de 0 (station vide) à 1 (station pleine). Le reducer calcule la moyenne, l'écart-type et le ratio d'échantillons valides.

### MR2 — Anomalies

Détecte trois types de problèmes : stations sans mise à jour depuis 30 min (NO_UPDATE), stations ouvertes mais sans vélo (ZERO_BIKES), stations dont toutes les bornes sont vides (FULL_STANDS). Produit un score de fiabilité par station.

### MR3 — Agrégats horaire/quartier

Croise l'heure (0-23) avec l'arrondissement (déterminé par heuristique GPS) pour calculer le 95e percentile du load_factor. Permet de prédire les pics de demande.

### MR4 — Heatmap stratégique

Calcule un CA potentiel par quartier (load_factor × places × 2€/jour × 365). Les quartiers dépassant 500k€ sont marqués priorité 1 pour l'investissement.

## Tables Hive

| Table | Source | Description |
|-------|--------|-------------|
| raw_stations | API JCDecaux | Données brutes JSON (JsonSerDe) |
| load_metrics | MR1 | Load factor par station |
| anomalies | MR2 | Score fiabilité par station |
| horaire_quartier | MR3 | P95 load factor par heure/quartier |
| heatmap | MR4 | CA potentiel par quartier |

## Interfaces web

| Service | URL | Description |
|---------|-----|-------------|
| HDFS | http://localhost:9870 | Navigateur de fichiers HDFS |
| YARN | http://localhost:8088 | Suivi des jobs MapReduce |
| Hive | http://localhost:10002 | Interface web Hive |
| Atlas | http://localhost:21000 | Gouvernance (login: admin/admin) |

## Notes techniques

- **Mac Apple Silicon (M1/M2/M3)** : les images Hadoop tournent en émulation amd64 via Rosetta. Des warnings "platform does not match" apparaissent, c'est normal. Atlas peut devenir instable après plusieurs heures — un restart de Docker Desktop résout le problème.
- **Python 3.5** : les containers Hadoop embarquent une vieille version de Debian (Stretch). Python 3.5 ne supporte pas les f-strings, d'où l'utilisation de `.format()` dans les mappers/reducers.
- **Clé API** : la clé JCDecaux est stockée dans `.env` (non versionné). Pour la récupérer : https://developer.jcdecaux.com/

## Stack technique

- Hadoop 3.2.1 (HDFS + YARN + MapReduce)
- Kafka (Confluent 7.0.1)
- Hive 3.1.3
- Apache Atlas 2.3.0
- Python 3 (scripts MapReduce et Kafka)
- Docker Compose

## Auteur

Jawad Berrhili — projet réalisé dans le cadre d'un brief Data Engineering (Siplon : promotion 2025-2027)

