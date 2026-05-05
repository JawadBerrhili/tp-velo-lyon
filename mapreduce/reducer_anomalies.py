#!/usr/bin/env python3
"""
MR2 - Reducer Anomalies
Regroupe les anomalies par station
Calcule un score de fiabilité
Sortie : station_id\tfiabilite_pourcent\tnb_anomalies\tderniere_panne
"""
import sys

current_station = None
nb_anomalies = 0
total_samples = 0
derniere_panne = ""

def emit(station_id, nb_anomalies, total_samples, derniere_panne):
    """Calcule et affiche la fiabilité d'une station"""
    if total_samples == 0:
        return

    # Fiabilité = pourcentage du temps sans anomalie
    # 100% = jamais de problème, 0% = toujours en panne
    fiabilite = int((total_samples - nb_anomalies) / total_samples * 100)

    print("{}\t{}%\t{}\t{}".format(station_id, fiabilite, nb_anomalies, derniere_panne))

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    station_id = parts[0]
    anomaly_type = parts[1]

    # Quand on change de station, on émet les résultats
    if current_station and current_station != station_id:
        emit(current_station, nb_anomalies, total_samples, derniere_panne)
        nb_anomalies = 0
        total_samples = 0
        derniere_panne = ""

    current_station = station_id
    total_samples += 1
    nb_anomalies += 1
    derniere_panne = anomaly_type

# Dernière station
emit(current_station, nb_anomalies, total_samples, derniere_panne)