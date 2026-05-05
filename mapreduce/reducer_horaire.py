#!/usr/bin/env python3
"""
MR3 - Reducer Horaire/Quartier
Regroupe par heure+quartier
Calcule le 95e percentile du load_factor
Sortie : heure\tquartier\tp95_load\tnb_stations\tcapacite_totale
"""
import sys
import math

current_key = None
load_factors = []
capacite_totale = 0
nb_stations = 0

def emit(key, load_factors, nb_stations, capacite_totale):
    """Calcule et affiche les métriques pour un créneau heure+quartier"""
    if not load_factors:
        return

    # Calcul du 95e percentile
    # On trie les load_factors et on prend la valeur à la position 95%
    # Exemple : 100 valeurs triées, le p95 est la 95e valeur
    # Ca donne le "pire cas réaliste" : dans 95% des cas, le load_factor
    # est en dessous de cette valeur
    sorted_lf = sorted(load_factors)
    index_95 = int(math.ceil(0.95 * len(sorted_lf))) - 1
    if index_95 < 0:
        index_95 = 0
    p95 = sorted_lf[index_95]

    # key contient "heure\tquartier" car Hadoop trie sur la clé complète
    print("{}\t{:.2f}\t{}\t{}".format(key, p95, nb_stations, capacite_totale))

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    # La clé de regroupement = heure + quartier
    key = "{}\t{}".format(parts[0], parts[1])
    load_factor = float(parts[2])
    capacite = int(parts[3])

    # Quand la clé change, on émet les résultats du groupe précédent
    if current_key and current_key != key:
        emit(current_key, load_factors, nb_stations, capacite_totale)
        load_factors = []
        capacite_totale = 0
        nb_stations = 0

    current_key = key
    load_factors.append(load_factor)
    capacite_totale += capacite
    nb_stations += 1

# Dernier groupe
emit(current_key, load_factors, nb_stations, capacite_totale)