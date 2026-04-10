"""
MR1 - Reducer Load Factor
Reçoit les lignes triées par station_id (Hadoop trie entre map et reduce)
Calcule par station : moyenne, écart-type, nb échantillons valides / total
Sortie : station_id\tavg_load_factor\tstd_load\tnb_valides/total
"""
import sys
import math

current_station = None
load_factors = []        # liste des load_factor des échantillons valides
total_samples = 0        # nombre total de lignes pour cette station
valid_samples = 0        # nombre de lignes avec status_valide=1

def emit(station_id, load_factors, valid_samples, total_samples):
    """Calcule et affiche les métriques pour une station"""
    if not load_factors:
        return

    # Moyenne des load_factor (uniquement sur les échantillons valides)
    avg = sum(load_factors) / len(load_factors)

    # Écart-type : mesure la variabilité du load_factor
    # Si l'écart-type est élevé, la station alterne entre vide et pleine
    # Si il est faible, la station est stable
    if len(load_factors) > 1:
        variance = sum((x - avg) ** 2 for x in load_factors) / len(load_factors)
        std = math.sqrt(variance)
    else:
        std = 0.0

    print(f"{station_id}\t{avg:.2f}\t{std:.2f}\t{valid_samples}/{total_samples}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    station_id = parts[0]
    load_factor = float(parts[2])
    status_valide = int(parts[3])

    # Hadoop trie les lignes par clé (station_id)
    # Quand la station change, on émet les résultats de la précédente
    if current_station and current_station != station_id:
        emit(current_station, load_factors, valid_samples, total_samples)
        load_factors = []
        total_samples = 0
        valid_samples = 0

    current_station = station_id
    total_samples += 1

    # On ne compte le load_factor que si l'échantillon est valide
    if status_valide == 1:
        load_factors.append(load_factor)
        valid_samples += 1

# Ne pas oublier la dernière station
emit(current_station, load_factors, valid_samples, total_samples)