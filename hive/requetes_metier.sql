-- ================================================
-- Requêtes métier - Data Lake Vélo Lyon
-- ================================================

-- Q1 : Quelles sont les 15 stations qui n'ont plus aucun vélo disponible en ce moment, en commençant par les plus grandes ?

SELECT number, name, bike_stands, available_bikes, available_bike_stands
FROM raw_stations
WHERE available_bikes = 0
ORDER BY bike_stands DESC
LIMIT 15;

-- -- Résultat Q1 :
-- +---------+----------------------------------------+--------------+------------------+------------------------+
-- | number  |                  name                  | bike_stands  | available_bikes  | available_bike_stands  |
-- +---------+----------------------------------------+--------------+------------------+------------------------+
-- | 6043    | 6043 - CITÉ INTERNATIONALE / CINÉMA    | 32           | 0                | 32                     |
-- | 3018    | 3018 - CRÉQUI / VOLTAIRE               | 30           | 0                | 29                     |
-- | 10053   | 10053 - ROSSELINI / 11 NOVEMBRE        | 30           | 0                | 30                     |
-- | 3068    | 3068 - FAURE / MEYNIS                  | 30           | 0                | 28                     |
-- | 3082    | 3082 - SAXE / MONCEY                   | 28           | 0                | 27                     |
-- | 10014   | 10014 - LAURENT BONNEVAY / ASTROBALLE  | 28           | 0                | 25                     |
-- | 7036    | 7036 - GRYPHE / MONTESQUIEU            | 25           | 0                | 0                      |
-- | 8040    | 8040 - AMBROISE PARÉ                   | 24           | 0                | 0                      |
-- | 10113   | 10113 - GARE DE VILLEURBANNE           | 22           | 0                | 22                     |
-- | 6044    | 6044 - CHARMETTES / BELLECOMBE         | 21           | 0                | 21                     |
-- | 2019    | 2019 - SUCRIERE                        | 20           | 0                | 0                      |
-- | 7026    | 7026 - BALDASSINI / GERLAND            | 20           | 0                | 0                      |
-- | 5054    | 5054 - CHAMPVERT                       | 19           | 0                | 18                     |
-- | 2025    | 2025 - MERCIÈRE / FERRANDIÈRE          | 18           | 0                | 16                     |
-- | 5007    | 5007 - PLACE DE TRION                  | 16           | 0                | 16                     |
-- +---------+----------------------------------------+--------------+------------------+------------------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q2 : Quels sont les 5 quartiers les plus en tension, en croisant le nombre de stations vides et leur capacité totale ?

SELECT quartier, 
  COUNT(CASE WHEN available_bikes = 0 THEN 1 END) as stations_vides,
  SUM(bike_stands) as capacite_totale
FROM (
  SELECT bike_stands, available_bikes,
    CASE
      WHEN position.lat BETWEEN 45.764 AND 45.775 AND position.lng BETWEEN 4.828 AND 4.838 THEN 'Lyon1'
      WHEN position.lat BETWEEN 45.738 AND 45.764 AND position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon2'
      WHEN position.lat BETWEEN 45.748 AND 45.770 AND position.lng BETWEEN 4.840 AND 4.880 THEN 'Lyon3'
      WHEN position.lat BETWEEN 45.775 AND 45.790 AND position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon4'
      WHEN position.lat BETWEEN 45.750 AND 45.770 AND position.lng BETWEEN 4.810 AND 4.828 THEN 'Lyon5'
      WHEN position.lat BETWEEN 45.770 AND 45.785 AND position.lng BETWEEN 4.840 AND 4.870 THEN 'Lyon6'
      WHEN position.lat BETWEEN 45.720 AND 45.748 AND position.lng BETWEEN 4.830 AND 4.870 THEN 'Lyon7'
      WHEN position.lat BETWEEN 45.725 AND 45.748 AND position.lng BETWEEN 4.870 AND 4.900 THEN 'Lyon8'
      WHEN position.lat BETWEEN 45.770 AND 45.800 AND position.lng BETWEEN 4.790 AND 4.820 THEN 'Lyon9'
      ELSE 'Autre'
    END as quartier
  FROM raw_stations
) t
GROUP BY quartier
ORDER BY stations_vides DESC, capacite_totale DESC
LIMIT 5;

-- -- -- Résultat Q2 :
-- +-----------+-----------------+------------------+
-- | quartier  | stations_vides  | capacite_totale  |
-- +-----------+-----------------+------------------+
-- | Autre     | 6               | 3049             |
-- | Lyon3     | 6               | 1988             |
-- | Lyon6     | 2               | 581              |
-- | Lyon2     | 1               | 1053             |
-- | Lyon7     | 1               | 921              |
-- +-----------+-----------------+------------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q3 : Le nombre de vélos disponibles à la station 2010 a-t-il augmenté ou diminué ces 10 dernières minutes ?

SELECT number, name, available_bikes, available_bike_stands,
  from_unixtime(CAST(last_update/1000 AS BIGINT)) as derniere_maj
FROM raw_stations
WHERE number = 2010;

-- -- -- -- Résultat Q3 :
-- +---------+----------------------------+------------------+------------------------+----------------------+
-- | number  |            name            | available_bikes  | available_bike_stands  |     derniere_maj     |
-- +---------+----------------------------+------------------+------------------------+----------------------+
-- | 2010    | 2010 - CONFLUENCE / DARSE  | 11               | 11                     | 2026-05-06 07:38:04  |
-- +---------+----------------------------+------------------+------------------------+----------------------+

-- Note : avec un seul snapshot de l'API, on ne voit qu'un seul point dans le temps.
-- Avec des données collectées en continu via Kafka, on verrait plusieurs lignes
-- et on pourrait comparer l'évolution des vélos disponibles sur les 10 dernières minutes.

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q4 : Quelles stations n'ont pas envoyé de données depuis plus de 2 heures, ou sont restées à zéro vélo pendant 4 heures d'affilée ?

SELECT number, name, status, available_bikes,
  from_unixtime(CAST(last_update/1000 AS BIGINT)) as derniere_maj,
  (unix_timestamp() - CAST(last_update/1000 AS BIGINT)) / 3600 as heures_sans_maj
FROM raw_stations
WHERE (unix_timestamp() - CAST(last_update/1000 AS BIGINT)) > 7200
   OR (available_bikes = 0 AND status = 'OPEN')
ORDER BY heures_sans_maj DESC;

-- -- -- -- -- -- Résultat Q4 :
-- +---------+----------------------------------------------+---------+------------------+----------------------+---------------------+
-- | number  |                     name                     | status  | available_bikes  |     derniere_maj     |   heures_sans_maj   |
-- +---------+----------------------------------------------+---------+------------------+----------------------+---------------------+
-- | 10116   | 10116 - BLUM / FAYS                          | OPEN    | 13               | 2026-05-06 07:28:37  | 2.0252777777777777  |
-- | 8052    | 8052 - CLINIQUE MONPLAISIR                   | OPEN    | 9                | 2026-05-06 07:28:37  | 2.0252777777777777  |
-- | 10063   | 10063 - PERRIN / JEAN JAURÈS                 | OPEN    | 11               | 2026-05-06 07:28:40  | 2.0244444444444443  |
-- | 11003   | 11003 - CALUIRE - LA ROCHETTE                | OPEN    | 6                | 2026-05-06 07:28:41  | 2.0241666666666664  |
-- | 4014    | 4014 - PHILIPPE DE LASSALLE / PILLEMENT      | OPEN    | 0                | 2026-05-06 07:28:43  | 2.0236111111111112  |
-- | 3091    | 3091 - HOPITAL NEUROLOGIQUE                  | OPEN    | 7                | 2026-05-06 07:28:44  | 2.0233333333333334  |
-- | 10113   | 10113 - GARE DE VILLEURBANNE                 | OPEN    | 0                | 2026-05-06 07:28:49  | 2.0219444444444443  |
-- | 13500   | 13500 - CHASSIEU - GENAS / PROGRES           | OPEN    | 9                | 2026-05-06 07:28:52  | 2.0211111111111113  |
-- | 555     | 0-555 - ATELIER VÉLO'V                       | OPEN    | 3                | 2026-05-06 07:28:53  | 2.0208333333333335  |
-- | 9013    | 9013 - QUAI PAUL SÉDALLIAN                   | OPEN    | 9                | 2026-05-06 07:28:58  | 2.0194444444444444  |
-- | 2013    | 2013 - CÉLESTINS                             | OPEN    | 3                | 2026-05-06 07:29:01  | 2.018611111111111   |
-- | 10110   | 10110 - MÉMOIRE ET SOCIÉTÉ                   | OPEN    | 12               | 2026-05-06 07:29:06  | 2.0172222222222222  |
-- | 5001    | 5001 - PLACE VARILLON (FUNICULAIRE ST JUST)  | OPEN    | 14               | 2026-05-06 07:29:06  | 2.0172222222222222  |
-- | 7025    | 7025 - LACOUR / ARTILLERIE                   | OPEN    | 17               | 2026-05-06 07:29:08  | 2.0166666666666666  |
-- | 19001   | 19001 - ST GENIS L. - MÉDIATHÈQUE            | OPEN    | 5                | 2026-05-06 07:29:10  | 2.016111111111111   |
-- | 10023   | 10023 - JAURES SAINT EXUPERY                 | OPEN    | 13               | 2026-05-06 07:29:15  | 2.0147222222222223  |
-- | 2041    | 2041 - SALA / CHARITÉ                        | OPEN    | 9                | 2026-05-06 07:29:17  | 2.0141666666666667  |
-- | 7045    | 7045 - PARC DES BERGES                       | OPEN    | 8                | 2026-05-06 07:29:28  | 2.011111111111111   |
-- | 3051    | 3051 - PLACE HENRI                           | OPEN    | 3                | 2026-05-06 07:29:30  | 2.0105555555555554  |
-- | 16003   | 16003 - VÉNISSIEUX - JOLIOT CURIE / SEMBAT   | OPEN    | 12               | 2026-05-06 07:29:53  | 2.004166666666667   |
-- | 3031    | 3031 - CORNEILLE / SERVIENT                  | OPEN    | 1                | 2026-05-06 07:29:55  | 2.0036111111111112  |
-- | 10071   | 10071 - LYCÉE MARIE CURIE                    | OPEN    | 4                | 2026-05-06 07:29:58  | 2.0027777777777778  |
-- | 2004    | 2004 - PERRACHE / CARNOT                     | OPEN    | 11               | 2026-05-06 07:29:59  | 2.0025              |
-- | 5026    | 5026 - RUE DE LA BALEINE                     | OPEN    | 13               | 2026-05-06 07:30:00  | 2.002222222222222   |
-- | 7053    | 7053 - JAURÈS / THIBAUDIÈRE                  | OPEN    | 6                | 2026-05-06 07:30:06  | 2.0005555555555556  |
-- | 3068    | 3068 - FAURE / MEYNIS                        | OPEN    | 0                | 2026-05-06 07:33:53  | 1.9375              |
-- | 10014   | 10014 - LAURENT BONNEVAY / ASTROBALLE        | OPEN    | 0                | 2026-05-06 07:35:58  | 1.9027777777777777  |
-- | 7026    | 7026 - BALDASSINI / GERLAND                  | OPEN    | 0                | 2026-05-06 07:36:15  | 1.8980555555555556  |
-- | 6044    | 6044 - CHARMETTES / BELLECOMBE               | OPEN    | 0                | 2026-05-06 07:36:25  | 1.8952777777777778  |
-- | 3018    | 3018 - CRÉQUI / VOLTAIRE                     | OPEN    | 0                | 2026-05-06 07:36:25  | 1.8952777777777778  |
-- | 5054    | 5054 - CHAMPVERT                             | OPEN    | 0                | 2026-05-06 07:37:06  | 1.883888888888889   |
-- | 10053   | 10053 - ROSSELINI / 11 NOVEMBRE              | OPEN    | 0                | 2026-05-06 07:37:17  | 1.8808333333333334  |
-- | 3085    | 3085 - SACRE COEUR                           | OPEN    | 0                | 2026-05-06 07:37:32  | 1.8766666666666667  |
-- | 5007    | 5007 - PLACE DE TRION                        | OPEN    | 0                | 2026-05-06 07:37:34  | 1.876111111111111   |
-- | 6043    | 6043 - CITÉ INTERNATIONALE / CINÉMA          | OPEN    | 0                | 2026-05-06 07:37:59  | 1.8691666666666666  |
-- | 2025    | 2025 - MERCIÈRE / FERRANDIÈRE                | OPEN    | 0                | 2026-05-06 07:38:20  | 1.8633333333333333  |
-- | 5016    | 5016 - POINT DU JOUR / GRANGES               | OPEN    | 0                | 2026-05-06 07:38:22  | 1.8627777777777779  |
-- | 3082    | 3082 - SAXE / MONCEY                         | OPEN    | 0                | 2026-05-06 07:38:28  | 1.8611111111111112  |
-- | 17006   | 17006 - SAINT FONS - ROCHETTES / CREST       | OPEN    | 0                | NULL                 | NULL                |
-- +---------+----------------------------------------------+---------+------------------+----------------------+---------------------+

-- La station 17006 a NULL pour la dernière mise à jour, c'est une station "zombie"

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q5 : Quels arrondissements affichent un taux de remplissage supérieur à 85 % avec moins de 15 places libres en moyenne ?

SELECT quartier, 
  ROUND(AVG(load_factor), 2) as avg_load,
  ROUND(AVG(places_libres), 1) as avg_places_libres
FROM (
  SELECT available_bike_stands as places_libres,
    CAST(available_bikes AS DOUBLE) / (available_bikes + available_bike_stands) as load_factor,
    CASE
      WHEN position.lat BETWEEN 45.764 AND 45.775 AND position.lng BETWEEN 4.828 AND 4.838 THEN 'Lyon1'
      WHEN position.lat BETWEEN 45.738 AND 45.764 AND position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon2'
      WHEN position.lat BETWEEN 45.748 AND 45.770 AND position.lng BETWEEN 4.840 AND 4.880 THEN 'Lyon3'
      WHEN position.lat BETWEEN 45.775 AND 45.790 AND position.lng BETWEEN 4.820 AND 4.840 THEN 'Lyon4'
      WHEN position.lat BETWEEN 45.750 AND 45.770 AND position.lng BETWEEN 4.810 AND 4.828 THEN 'Lyon5'
      WHEN position.lat BETWEEN 45.770 AND 45.785 AND position.lng BETWEEN 4.840 AND 4.870 THEN 'Lyon6'
      WHEN position.lat BETWEEN 45.720 AND 45.748 AND position.lng BETWEEN 4.830 AND 4.870 THEN 'Lyon7'
      WHEN position.lat BETWEEN 45.725 AND 45.748 AND position.lng BETWEEN 4.870 AND 4.900 THEN 'Lyon8'
      WHEN position.lat BETWEEN 45.770 AND 45.800 AND position.lng BETWEEN 4.790 AND 4.820 THEN 'Lyon9'
      ELSE 'Autre'
    END as quartier
  FROM raw_stations
  WHERE (available_bikes + available_bike_stands) > 0
) t
GROUP BY quartier
HAVING AVG(load_factor) > 0.85 AND AVG(places_libres) < 15;

-- -- -- -- -- Résultat Q5 :
-- +-----------+-----------+--------------------+
-- | quartier  | avg_load  | avg_places_libres  |
-- +-----------+-----------+--------------------+
-- +-----------+-----------+--------------------+

-- Aucun arrondissement ne remplit les deux conditions simultanément.
-- Avec des données collectées aux heures de pointe (18h-19h) via Kafka,
-- on observerait probablement des quartiers en tension.

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

-- Q6 : Quelle proportion des stations dispose de coordonnées GPS et d'un statut valides ?

SELECT
  COUNT(*) as total_stations,
  COUNT(CASE WHEN position.lat IS NOT NULL AND position.lng IS NOT NULL THEN 1 END) as avec_gps,
  COUNT(CASE WHEN status IS NOT NULL THEN 1 END) as avec_statut,
  ROUND(COUNT(CASE WHEN position.lat IS NOT NULL AND position.lng IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_gps,
  ROUND(COUNT(CASE WHEN status IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_statut
FROM raw_stations;

-- -- -- -- -- Résultat Q6 :
-- +-----------------+-----------+--------------+----------+-------------+
-- | total_stations  | avec_gps  | avec_statut  | pct_gps  | pct_statut  |
-- +-----------------+-----------+--------------+----------+-------------+
-- | 448             | 448       | 448          | 100.0    | 100.0       |
-- +-----------------+-----------+--------------+----------+-------------+

-- +---------+----------------------------------------+--------+------------------+----------------------+-----------------+

