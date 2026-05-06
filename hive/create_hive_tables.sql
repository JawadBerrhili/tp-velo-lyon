-- ================================================
-- Tables Hive externes - Data Lake Vélo Lyon
-- ================================================

-- Ajout du SerDe JSON pour lire les données brutes
ADD JAR /opt/hive/lib/hive-hcatalog-core-3.1.3.jar;

-- Table 1 : Données brutes des stations (JSON)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_stations (
  number INT,
  contract_name STRING,
  name STRING,
  address STRING,
  banking BOOLEAN,
  bonus BOOLEAN,
  bike_stands INT,
  available_bike_stands INT,
  available_bikes INT,
  status STRING,
  last_update BIGINT,
  position STRUCT<lat:DOUBLE, lng:DOUBLE>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data-lake/raw/velo_lyon';

-- Table 2 : Résultats MR1 - Load Factor par station
CREATE EXTERNAL TABLE IF NOT EXISTS load_metrics (
  station_id STRING,
  avg_load_factor DOUBLE,
  std_load DOUBLE,
  samples_ratio STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data-lake/processed/load_metrics';

-- Table 3 : Résultats MR2 - Anomalies par station
CREATE EXTERNAL TABLE IF NOT EXISTS anomalies (
  station_id STRING,
  fiabilite STRING,
  nb_anomalies INT,
  derniere_panne STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data-lake/processed/anomalies';

-- Table 4 : Résultats MR3 - Agrégats horaire/quartier
CREATE EXTERNAL TABLE IF NOT EXISTS horaire_quartier (
  heure STRING,
  quartier STRING,
  p95_load DOUBLE,
  nb_stations INT,
  capacite_totale INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data-lake/processed/horaire_quartier';

-- Table 5 : Résultats MR4 - Heatmap stratégique
CREATE EXTERNAL TABLE IF NOT EXISTS heatmap (
  quartier STRING,
  stations_nb INT,
  capacite_totale INT,
  ca_potentiel STRING,
  priorite INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data-lake/processed/heatmap';