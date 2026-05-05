#!/usr/bin/env python3
"""
MR4 - Reducer Heatmap Stratégique
Regroupe par quartier
Calcule le CA potentiel total et la priorité d'investissement
Sortie : quartier\tstations_nb\tcapacite_totale\tca_potentiel_total\tpriorite
"""
import sys

current_quartier = None
stations_nb = 0
capacite_totale = 0
ca_potentiel_total = 0

def emit(quartier, stations_nb, capacite_totale, ca_potentiel_total):
    """Affiche les métriques agrégées pour un quartier"""
    if not quartier:
        return

    # Formatage du CA pour lisibilité
    # 2100000 → "2.1M€", 450000 → "450k€"
    if ca_potentiel_total >= 1000000:
        ca_str = "{:.1f}M".format(ca_potentiel_total / 1000000)
    elif ca_potentiel_total >= 1000:
        ca_str = "{}k".format(int(ca_potentiel_total / 1000))
    else:
        ca_str = str(ca_potentiel_total)

    # Priorité d'investissement : 1 si le CA potentiel dépasse 500k€
    # C'est le seuil défini dans le brief pour justifier un investissement
    priorite = 1 if ca_potentiel_total > 500000 else 0

    print("{}\t{}\t{}\t{}\t{}".format(
        quartier, stations_nb, capacite_totale, ca_str, priorite))

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    quartier = parts[0]
    utilisation = int(parts[1])
    ca_potentiel = int(parts[2])

    # Quand le quartier change, on émet les résultats du précédent
    if current_quartier and current_quartier != quartier:
        emit(current_quartier, stations_nb, capacite_totale, ca_potentiel_total)
        stations_nb = 0
        capacite_totale = 0
        ca_potentiel_total = 0

    current_quartier = quartier
    stations_nb += 1
    ca_potentiel_total += ca_potentiel

    # On recalcule la capacité depuis l'utilisation et le CA
    # ca_potentiel = load_factor × total × 2 × 365
    # Mais on n'a pas "total" directement ici, on a besoin de l'extraire
    # Comme utilisation = load_factor × 100 et ca = load_factor × total × 730
    # Si utilisation > 0 : total = ca_potentiel / (utilisation/100 × 730)
    # Sinon on ne peut pas calculer (station vide)
    if utilisation > 0:
        total_station = int(ca_potentiel / (utilisation / 100.0 * 730))
        capacite_totale += total_station
    # Si utilisation = 0, le CA est aussi 0, la station n'apporte rien

# Dernier quartier
emit(current_quartier, stations_nb, capacite_totale, ca_potentiel_total)