-- ================================================
-- Requêtes métier - Data Lake Vélo Lyon
-- ================================================

-- Q1 : Quelles sont les 15 stations qui n'ont plus aucun vélo disponible en ce moment, en commençant par les plus grandes ?

SELECT DISTINCT number, name, bike_stands, available_bikes, available_bike_stands
FROM raw_stations t
INNER JOIN (
  SELECT number as snum, MAX(last_update) as max_update
  FROM raw_stations
  GROUP BY number
) latest ON t.number = latest.snum AND t.last_update = latest.max_update
WHERE t.available_bikes = 0
ORDER BY bike_stands DESC
LIMIT 15;

-- -- -- Résultat Q1 :
-- +---------+--------------------------------------------+--------------+------------------+------------------------+
-- | number  |                    name                    | bike_stands  | available_bikes  | available_bike_stands  |
-- +---------+--------------------------------------------+--------------+------------------+------------------------+
-- | 12004   | 12004 - VAULX - HÔTEL DE VILLE             | 30           | 0                | 30                     |
-- | 7036    | 7036 - GRYPHE / MONTESQUIEU                | 25           | 0                | 0                      |
-- | 7045    | 7045 - PARC DES BERGES                     | 25           | 0                | 25                     |
-- | 10124   | 10124 - SALENGRO / YVONNE                  | 24           | 0                | 23                     |
-- | 8040    | 8040 - AMBROISE PARÉ                       | 24           | 0                | 0                      |
-- | 7056    | 7056 - PLACE RASPAIL                       | 22           | 0                | 21                     |
-- | 7026    | 7026 - BALDASSINI / GERLAND                | 20           | 0                | 0                      |
-- | 5055    | 5055 - BUYER / APOLLINAIRE                 | 20           | 0                | 20                     |
-- | 4011    | 4011 - PLACE JOANNÈS AMBRE                 | 20           | 0                | 20                     |
-- | 2019    | 2019 - SUCRIERE                            | 20           | 0                | 0                      |
-- | 3058    | 3058 - PLACE DU LAC                        | 19           | 0                | 19                     |
-- | 5053    | 5053 - SAINT-JUST                          | 18           | 0                | 18                     |
-- | 5002    | 5002 - PLACE DES COMPAGNONS DE LA CHANSON  | 18           | 0                | 17                     |
-- | 1005    | 1005 - MEISSONNIER                         | 16           | 0                | 13                     |
-- | 3088    | 3088 - PLACE GUICHARD                      | 16           | 0                | 16                     |
-- +---------+--------------------------------------------+--------------+------------------+------------------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q2 : Quels sont les 5 quartiers les plus en tension, en croisant le nombre de stations vides et leur capacité totale ?

SELECT quartier,
  COUNT(CASE WHEN available_bikes = 0 THEN 1 END) as stations_vides,
  SUM(bike_stands) as capacite_totale
FROM (
  SELECT DISTINCT t.number, t.bike_stands, t.available_bikes,
    CASE
      WHEN t.position.lat BETWEEN 45.764 AND 45.775 AND t.position.lng BETWEEN 4.828 AND 4.838 THEN 'Lyon1'
      WHEN t.position.lat BETWEEN 45.738 AND 45.764 AND t.position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon2'
      WHEN t.position.lat BETWEEN 45.748 AND 45.770 AND t.position.lng BETWEEN 4.840 AND 4.880 THEN 'Lyon3'
      WHEN t.position.lat BETWEEN 45.775 AND 45.790 AND t.position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon4'
      WHEN t.position.lat BETWEEN 45.750 AND 45.770 AND t.position.lng BETWEEN 4.810 AND 4.828 THEN 'Lyon5'
      WHEN t.position.lat BETWEEN 45.770 AND 45.785 AND t.position.lng BETWEEN 4.840 AND 4.870 THEN 'Lyon6'
      WHEN t.position.lat BETWEEN 45.720 AND 45.748 AND t.position.lng BETWEEN 4.830 AND 4.870 THEN 'Lyon7'
      WHEN t.position.lat BETWEEN 45.725 AND 45.748 AND t.position.lng BETWEEN 4.870 AND 4.900 THEN 'Lyon8'
      WHEN t.position.lat BETWEEN 45.770 AND 45.800 AND t.position.lng BETWEEN 4.790 AND 4.820 THEN 'Lyon9'
      ELSE 'Autre'
    END as quartier
  FROM raw_stations t
  INNER JOIN (
    SELECT number as snum, MAX(last_update) as max_update
    FROM raw_stations
    GROUP BY number
  ) latest ON t.number = latest.snum AND t.last_update = latest.max_update
) sub
GROUP BY quartier
ORDER BY stations_vides DESC, capacite_totale DESC
LIMIT 5;

-- -- -- -- Résultat Q2 :
-- +-----------+-----------------+------------------+
-- | quartier  | stations_vides  | capacite_totale  |
-- +-----------+-----------------+------------------+
-- | Autre     | 8               | 3034             |
-- | Lyon3     | 4               | 1988             |
-- | Lyon7     | 3               | 921              |
-- | Lyon8     | 1               | 432              |
-- | Lyon1     | 1               | 362              |
-- +-----------+-----------------+------------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q3 : Le nombre de vélos disponibles à la station 2010 a-t-il augmenté ou diminué ces 10 dernières minutes ?

SELECT DISTINCT number, name, available_bikes, available_bike_stands,
  from_unixtime(CAST(last_update/1000 AS BIGINT)) as derniere_maj
FROM raw_stations
WHERE number = 2010
ORDER BY derniere_maj DESC;

-- -- -- -- -- Résultat Q3 :
-- +---------+----------------------------+------------------+------------------------+----------------------+
-- | number  |            name            | available_bikes  | available_bike_stands  |     derniere_maj     |
-- +---------+----------------------------+------------------+------------------------+----------------------+
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:38:51  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:28:52  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:24:53  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 7                | 15                     | 2026-05-06 12:22:17  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:21:17  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:18:47  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:08:46  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 12:02:17  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 7                | 15                     | 2026-05-06 11:59:30  |
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 8                | 14                     | 2026-05-06 11:48:43  |
-- +---------+----------------------------+------------------+------------------------+----------------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q4 : Quelles stations n'ont pas envoyé de données depuis plus de 2 heures, ou sont restées à zéro vélo pendant 4 heures d'affilée ?

SELECT DISTINCT t.number, t.name, t.status, t.available_bikes,
  from_unixtime(CAST(t.last_update/1000 AS BIGINT)) as derniere_maj,
  (unix_timestamp() - CAST(t.last_update/1000 AS BIGINT)) / 3600 as heures_sans_maj
FROM raw_stations t
INNER JOIN (
  SELECT number as snum, MAX(last_update) as max_update
  FROM raw_stations
  GROUP BY number
) latest ON t.number = latest.snum AND t.last_update = latest.max_update
WHERE (unix_timestamp() - CAST(t.last_update/1000 AS BIGINT)) > 7200
   OR (t.available_bikes = 0 AND t.status = 'OPEN')
ORDER BY heures_sans_maj DESC
LIMIT 20;

-- -- -- -- -- -- -- Résultat Q4 :
-- +-----------+--------------------------------------------+-----------+--------------------+----------------------+---------------------+
-- | t.number  |                   t.name                   | t.status  | t.available_bikes  |     derniere_maj     |   heures_sans_maj   |
-- +-----------+--------------------------------------------+-----------+--------------------+----------------------+---------------------+
-- | 5044      | 5044 - CHAMPVERT                           | OPEN      | 0                  | 2026-05-06 12:36:33  | 0.7905555555555556  |
-- | 7026      | 7026 - BALDASSINI / GERLAND                | OPEN      | 0                  | 2026-05-06 12:36:39  | 0.7888888888888889  |
-- | 10022     | 10022 - PRIMAT / DUCROIZE                  | OPEN      | 0                  | 2026-05-06 12:37:29  | 0.775               |
-- | 3058      | 3058 - PLACE DU LAC                        | OPEN      | 0                  | 2026-05-06 12:38:19  | 0.7611111111111111  |
-- | 10124     | 10124 - SALENGRO / YVONNE                  | OPEN      | 0                  | 2026-05-06 12:38:27  | 0.7588888888888888  |
-- | 7056      | 7056 - PLACE RASPAIL                       | OPEN      | 0                  | 2026-05-06 12:38:32  | 0.7575              |
-- | 12004     | 12004 - VAULX - HÔTEL DE VILLE             | OPEN      | 0                  | 2026-05-06 12:38:47  | 0.7533333333333333  |
-- | 3088      | 3088 - PLACE GUICHARD                      | OPEN      | 0                  | 2026-05-06 12:38:57  | 0.7505555555555555  |
-- | 1005      | 1005 - MEISSONNIER                         | OPEN      | 0                  | 2026-05-06 12:39:00  | 0.7497222222222222  |
-- | 5002      | 5002 - PLACE DES COMPAGNONS DE LA CHANSON  | OPEN      | 0                  | 2026-05-06 12:39:20  | 0.7441666666666666  |
-- | 8034      | 8034 - BERTHELOT / VILLON                  | OPEN      | 0                  | 2026-05-06 12:39:41  | 0.7383333333333333  |
-- | 8052      | 8052 - CLINIQUE MONPLAISIR                 | OPEN      | 0                  | 2026-05-06 12:39:55  | 0.7344444444444445  |
-- | 7045      | 7045 - PARC DES BERGES                     | OPEN      | 0                  | 2026-05-06 12:40:36  | 0.7230555555555556  |
-- | 5053      | 5053 - SAINT-JUST                          | OPEN      | 0                  | 2026-05-06 12:41:33  | 0.7072222222222222  |
-- | 5055      | 5055 - BUYER / APOLLINAIRE                 | OPEN      | 0                  | 2026-05-06 12:41:41  | 0.705               |
-- | 4011      | 4011 - PLACE JOANNÈS AMBRE                 | OPEN      | 0                  | 2026-05-06 12:41:47  | 0.7033333333333334  |
-- +-----------+--------------------------------------------+-----------+--------------------+----------------------+---------------------+

-- La station 17006 a NULL pour la dernière mise à jour, c'est une station "zombie"

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q5 : Quels arrondissements affichent un taux de remplissage supérieur à 85 % avec moins de 15 places libres en moyenne ?

SELECT quartier,
  ROUND(AVG(load_factor), 2) as avg_load,
  ROUND(AVG(places_libres), 1) as avg_places_libres
FROM (
  SELECT DISTINCT t.number, t.available_bike_stands as places_libres,
    CAST(t.available_bikes AS DOUBLE) / (t.available_bikes + t.available_bike_stands) as load_factor,
    CASE
      WHEN t.position.lat BETWEEN 45.764 AND 45.775 AND t.position.lng BETWEEN 4.828 AND 4.838 THEN 'Lyon1'
      WHEN t.position.lat BETWEEN 45.738 AND 45.764 AND t.position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon2'
      WHEN t.position.lat BETWEEN 45.748 AND 45.770 AND t.position.lng BETWEEN 4.840 AND 4.880 THEN 'Lyon3'
      WHEN t.position.lat BETWEEN 45.775 AND 45.790 AND t.position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon4'
      WHEN t.position.lat BETWEEN 45.750 AND 45.770 AND t.position.lng BETWEEN 4.810 AND 4.828 THEN 'Lyon5'
      WHEN t.position.lat BETWEEN 45.770 AND 45.785 AND t.position.lng BETWEEN 4.840 AND 4.870 THEN 'Lyon6'
      WHEN t.position.lat BETWEEN 45.720 AND 45.748 AND t.position.lng BETWEEN 4.830 AND 4.870 THEN 'Lyon7'
      WHEN t.position.lat BETWEEN 45.725 AND 45.748 AND t.position.lng BETWEEN 4.870 AND 4.900 THEN 'Lyon8'
      WHEN t.position.lat BETWEEN 45.770 AND 45.800 AND t.position.lng BETWEEN 4.790 AND 4.820 THEN 'Lyon9'
      ELSE 'Autre'
    END as quartier
  FROM raw_stations t
  INNER JOIN (
    SELECT number as snum, MAX(last_update) as max_update
    FROM raw_stations
    GROUP BY number
  ) latest ON t.number = latest.snum AND t.last_update = latest.max_update
  WHERE (t.available_bikes + t.available_bike_stands) > 0
) sub
GROUP BY quartier
HAVING AVG(load_factor) > 0.85 AND AVG(places_libres) < 15;

-- -- -- -- -- Résultat Q5 :
-- +-----------+-----------+--------------------+
-- | quartier  | avg_load  | avg_places_libres  |
-- +-----------+-----------+--------------------+
-- +-----------+-----------+--------------------+

-- Aucun arrondissement ne remplit les deux conditions simultanément.
-- Les données couvrent la tranche 11h-12h40, une période creuse.
-- Aux heures de pointe (17h-19h), on observerait probablement des quartiers en tension.

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q6 : Quelle proportion des stations dispose de coordonnées GPS et d'un statut valides ?

SELECT
  COUNT(*) as total_stations,
  COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as avec_gps,
  COUNT(CASE WHEN status IS NOT NULL THEN 1 END) as avec_statut,
  ROUND(COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_gps,
  ROUND(COUNT(CASE WHEN status IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_statut
FROM (
  SELECT DISTINCT t.number, t.position.lat as lat, t.position.lng as lng, t.status
  FROM raw_stations t
  INNER JOIN (
    SELECT number as snum, MAX(last_update) as max_update
    FROM raw_stations
    GROUP BY number
  ) latest ON t.number = latest.snum AND t.last_update = latest.max_update
) sub;

-- -- -- -- -- Résultat Q6 :
-- +-----------------+-----------+--------------+----------+-------------+
-- | total_stations  | avec_gps  | avec_statut  | pct_gps  | pct_statut  |
-- +-----------------+-----------+--------------+----------+-------------+
-- | 447             | 447       | 447          | 100.0    | 100.0       |
-- +-----------------+-----------+--------------+----------+-------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

