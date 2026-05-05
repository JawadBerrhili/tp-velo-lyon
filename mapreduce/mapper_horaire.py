#!/usr/bin/env python3
"""
MR3 - Mapper Horaire/Quartier
Lit un JSON par ligne sur stdin
Extrait l'heure et détermine le quartier (arrondissement) via les coordonnées GPS
Sortie : heure\tquartier\tload_factor\tcapacite
"""
import sys
import json
import time

def get_arrondissement(lat, lng):
    """
    Heuristique pour déterminer l'arrondissement de Lyon
    à partir des coordonnées GPS.
    Lyon est organisé du nord au sud et d'est en ouest.
    Ces bornes sont approximatives mais suffisantes pour le TP.
    """
    # Lyon 1 : Presqu'île nord (Hôtel de Ville, Terreaux)
    if 45.764 <= lat <= 45.775 and 4.828 <= lng <= 4.838:
        return "Lyon1"

    # Lyon 2 : Presqu'île sud (Bellecour, Confluence)
    if 45.738 <= lat <= 45.764 and 4.820 <= lng <= 4.840:
        return "Lyon2"

    # Lyon 3 : Rive gauche centre (Part-Dieu, Préfecture)
    if 45.748 <= lat <= 45.770 and 4.840 <= lng <= 4.880:
        return "Lyon3"

    # Lyon 4 : Croix-Rousse
    if 45.775 <= lat <= 45.790 and 4.820 <= lng <= 4.840:
        return "Lyon4"

    # Lyon 5 : Vieux Lyon, Fourvière
    if 45.750 <= lat <= 45.770 and 4.810 <= lng <= 4.828:
        return "Lyon5"

    # Lyon 6 : Parc de la Tête d'Or, Brotteaux
    if 45.770 <= lat <= 45.785 and 4.840 <= lng <= 4.870:
        return "Lyon6"

    # Lyon 7 : Gerland, Jean Macé
    if 45.720 <= lat <= 45.748 and 4.830 <= lng <= 4.870:
        return "Lyon7"

    # Lyon 8 : Monplaisir, Mermoz
    if 45.725 <= lat <= 45.748 and 4.870 <= lng <= 4.900:
        return "Lyon8"

    # Lyon 9 : Vaise, La Duchère
    if 45.770 <= lat <= 45.800 and 4.790 <= lng <= 4.820:
        return "Lyon9"

    # Hors Lyon ou zone non identifiée
    return "Autre"

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        station = json.loads(line)
    except (ValueError, KeyError):
        continue

    bikes = station.get("available_bikes")
    stands = station.get("available_bike_stands")
    total = station.get("bike_stands")
    last_update = station.get("last_update")
    position = station.get("position", {})
    lat = position.get("lat")
    lng = position.get("lng")

    if None in (bikes, stands, total, last_update, lat, lng):
        continue

    # Calcul du load_factor (même formule que MR1)
    denominator = bikes + stands
    if denominator == 0:
        continue
    load_factor = round(bikes / denominator, 3)

    # Extraction de l'heure (0-23) depuis le timestamp
    # On convertit les millisecondes en secondes puis on extrait l'heure
    timestamp_sec = int(last_update / 1000)
    heure = time.strftime("%H", time.localtime(timestamp_sec))

    # Détermination du quartier via les coordonnées GPS
    quartier = get_arrondissement(lat, lng)

    print("{}\t{}\t{}\t{}".format(heure, quartier, load_factor, total))