#!/usr/bin/env python3
"""
MR4 - Mapper Heatmap Stratégique
Lit un JSON par ligne sur stdin
Calcule l'utilisation et le CA potentiel par station
Sortie : quartier\tutilisation\tca_potentiel\tnom_station
"""
import sys
import json


def get_arrondissement(lat, lng):
    """
    Même heuristique que MR3 pour déterminer l'arrondissement
    à partir des coordonnées GPS
    """
    if 45.764 <= lat <= 45.775 and 4.828 <= lng <= 4.838:
        return "Lyon1"
    if 45.738 <= lat <= 45.764 and 4.820 <= lng <= 4.840:
        return "Lyon2"
    if 45.748 <= lat <= 45.770 and 4.840 <= lng <= 4.880:
        return "Lyon3"
    if 45.775 <= lat <= 45.790 and 4.820 <= lng <= 4.840:
        return "Lyon4"
    if 45.750 <= lat <= 45.770 and 4.810 <= lng <= 4.828:
        return "Lyon5"
    if 45.770 <= lat <= 45.785 and 4.840 <= lng <= 4.870:
        return "Lyon6"
    if 45.720 <= lat <= 45.748 and 4.830 <= lng <= 4.870:
        return "Lyon7"
    if 45.725 <= lat <= 45.748 and 4.870 <= lng <= 4.900:
        return "Lyon8"
    if 45.770 <= lat <= 45.800 and 4.790 <= lng <= 4.820:
        return "Lyon9"
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
    name = station.get("name", "INCONNU")
    position = station.get("position", {})
    lat = position.get("lat")
    lng = position.get("lng")

    if None in (bikes, stands, total, lat, lng):
        continue

    # Calcul du load_factor (même formule que MR1 et MR3)
    denominator = bikes + stands
    if denominator == 0:
        continue
    load_factor = bikes / denominator

    # Utilisation en pourcentage
    # load_factor de 0.87 → utilisation de 87%
    utilisation = int(load_factor * 100)

    # Chiffre d'affaires potentiel annuel
    # Formule du brief : utilisation × nombre de places × 2€/jour × 365 jours
    # Exemple : 87% × 22 places × 2€ × 365 = 13 981€
    # Ca représente combien la station "rapporterait" si chaque vélo utilisé
    # générait 2€ par jour (abonnement, ticket, etc.)
    ca_potentiel = int(load_factor * total * 2 * 365)

    # Détermination du quartier
    quartier = get_arrondissement(lat, lng)

    # On nettoie le nom de la station (enlever le numéro au début)
    # "2010 - CONFLUENCE / DARSE" → "CONFLUENCE / DARSE"
    nom_clean = name.split(" - ", 1)[-1] if " - " in name else name

    print("{}\t{}\t{}\t{}".format(quartier, utilisation, ca_potentiel, nom_clean))