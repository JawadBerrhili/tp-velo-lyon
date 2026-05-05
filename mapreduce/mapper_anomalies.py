#!/usr/bin/env python3
"""
MR2 - Mapper Anomalies
Lit un JSON par ligne sur stdin
Détecte 3 types d'anomalies par station
Sortie : station_id\tanomaly_type\ttimestamp\tage_last_update
"""
import sys
import json
import time

# Timestamp actuel en secondes (moment où le job tourne)
now = int(time.time())

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        station = json.loads(line)
    except (ValueError, KeyError):
        continue

    station_id = station.get("number")
    bikes = station.get("available_bikes")
    stands = station.get("available_bike_stands")
    total = station.get("bike_stands")
    status = station.get("status")
    last_update = station.get("last_update")

    if None in (station_id, bikes, stands, total, status, last_update):
        continue

    # Timestamp en secondes (l'API donne des millisecondes)
    timestamp = int(last_update / 1000)

    # Age en secondes depuis la dernière mise à jour
    age = now - timestamp

    # Anomalie 1 : NO_UPDATE
    # La station n'a pas communiqué depuis plus de 30 minutes (1800 secondes)
    # Ca peut indiquer une panne du capteur ou de la borne
    if age > 1800:
        print("{}\t{}\t{}\t{}".format(station_id, "NO_UPDATE", timestamp, age))

    # Anomalie 2 : ZERO_BIKES
    # La station est marquée ouverte mais a 0 vélo disponible
    # Les usagers arrivent devant une station vide
    if status == "OPEN" and bikes == 0:
        print("{}\t{}\t{}\t{}".format(station_id, "ZERO_BIKES", timestamp, age))

    # Anomalie 3 : FULL_STANDS
    # Toutes les bornes sont libres = aucun vélo sur la station
    # Equivalent à une station fantôme
    if stands == total and total > 0:
        print("{}\t{}\t{}\t{}".format(station_id, "FULL_STANDS", timestamp, age))