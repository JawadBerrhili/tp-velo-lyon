#!/usr/bin/env python3
"""
Kafka Consumer - Vélo Lyon
Lit les messages du topic 'velo_lyon_raw' dans Kafka
et les écrit dans un fichier JSON ligne par ligne (un JSON par ligne)
Ce fichier pourra ensuite être chargé dans HDFS pour les jobs MapReduce
"""
import json
import time
import os
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "velo_lyon_raw"

# Le fichier de sortie : dans le dossier data/ de ton projet
# C'est le même format que stations_lines.json (un JSON par ligne)
# sauf qu'il contient TOUS les snapshots collectés par le producer
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "kafka_collected.json")


def create_consumer():
    """
    Crée une connexion au broker Kafka pour LIRE les messages.

    bootstrap_servers : adresse du broker Kafka (même que le producer)

    auto_offset_reset='earliest' : quand le consumer démarre pour la première
    fois, il lit TOUS les messages depuis le début du topic, pas seulement
    les nouveaux. Comme ça on récupère les données que le producer a déjà envoyées.
    Si on mettait 'latest', on ne verrait que les messages envoyés APRÈS
    le démarrage du consumer.

    enable_auto_commit=True : Kafka garde en mémoire où le consumer en est
    dans sa lecture. Si tu arrêtes et relances le consumer, il reprend là où
    il s'était arrêté au lieu de relire tout depuis le début.

    group_id='velo_lyon_consumer' : identifie ce consumer. Si tu lançais
    plusieurs consumers avec le même group_id, ils se partageraient les
    messages. Avec un group_id unique, chaque consumer lit tous les messages.

    value_deserializer : l'inverse du serializer du producer.
    Le producer convertit dict → JSON → bytes.
    Le consumer convertit bytes → JSON → dict.

    consumer_timeout_ms=10000 : si aucun nouveau message n'arrive pendant
    10 secondes, le consumer s'arrête au lieu de rester bloqué indéfiniment.
    Ça permet de lancer le consumer, récupérer tout ce qui est disponible,
    et sortir proprement.
    """
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="velo_lyon_consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000
    )


def main():
    """
    Logique principale :
    1. Connecte au topic Kafka
    2. Lit tous les messages disponibles
    3. Écrit chaque message (une station) comme une ligne JSON dans le fichier
    4. S'arrête quand il n'y a plus de messages (timeout 10s)
    """
    print("Démarrage du consumer Kafka...")
    print("Topic: {}".format(TOPIC))
    print("Fichier de sortie: {}".format(OUTPUT_FILE))
    print("-" * 40)

    consumer = create_consumer()
    count = 0

    # 'a' = append (ajout à la fin du fichier)
    # Si le fichier existe déjà, on ajoute à la suite
    # Si il n'existe pas, il est créé
    # encoding='utf-8' pour gérer les accents dans les noms de stations
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for message in consumer:
            # message.value contient le dictionnaire Python de la station
            # (déjà désérialisé grâce au value_deserializer)
            station = message.value

            # On écrit la station en JSON sur une seule ligne
            # ensure_ascii=False pour garder les accents
            # Le \n à la fin fait passer à la ligne suivante
            f.write(json.dumps(station, ensure_ascii=False) + "\n")
            count += 1

            # Affiche la progression tous les 1000 messages
            if count % 1000 == 0:
                print("[{}] {} messages lus...".format(
                    time.strftime("%H:%M:%S"), count))

    print("-" * 40)
    print("Terminé : {} messages écrits dans {}".format(count, OUTPUT_FILE))

    consumer.close()


if __name__ == "__main__":
    main()