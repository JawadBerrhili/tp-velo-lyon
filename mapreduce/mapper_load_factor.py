"""
MR1 - Mapper Load Factor
Lit un JSON par ligne sur stdin (une station par ligne)
Calcule le load_factor et vérifie la validité des données
Sortie : station_id\ttimestamp\tload_factor\tstatus_valide
"""
import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Gestion erreurs : JSON malformé → on ignore la ligne (pas de crash)
    try:
        station = json.loads(line)
    except json.JSONDecodeError:
        continue

    # Extraction des champs dont on a besoin
    station_id = station.get("number")
    bikes = station.get("available_bikes")
    stands = station.get("available_bike_stands")
    total = station.get("bike_stands")
    status = station.get("status")
    last_update = station.get("last_update")

    # Si un champ est manquant, on ignore la ligne
    if None in (station_id, bikes, stands, total, status, last_update):
        continue

    # Calcul du load_factor : proportion de vélos par rapport à la capacité
    # Exemple : 8 vélos, 14 places vides → 8 / (8+14) = 0.36
    # 0 = station vide, 1 = station pleine de vélos
    denominator = bikes + stands
    if denominator == 0:
        continue
    load_factor = round(bikes / denominator, 3)

    # Validation : la station est considérée "valide" si :
    # - elle est OPEN (pas en maintenance)
    # - le nombre de vélos est cohérent (entre 0 et la capacité totale)
    status_valide = 1 if (status == "OPEN" and 0 <= bikes <= total) else 0

    # Conversion timestamp : l'API donne des millisecondes, on garde les secondes
    timestamp = int(last_update / 1000)

    # Sortie tab-separated comme demandé par le brief
    print(f"{station_id}\t{timestamp}\t{load_factor}\t{status_valide}")