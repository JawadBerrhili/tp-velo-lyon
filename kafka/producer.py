#!/usr/bin/env python3
"""
Kafka Producer - Vélo Lyon
Appelle l'API JCDecaux toutes les 60 secondes
et envoie chaque station dans le topic Kafka 'velo_lyon_raw'
"""
import json
import time
import os
import requests
from kafka import KafkaProducer

# Configuration
# La clé API est lue depuis le fichier .env ou directement ici
API_KEY = os.getenv("JCDECAUX_API_KEY", "1aa8ac3920040d2d04af0e1e2e34f5cc0586a7d7")
API_URL = "https://api.jcdecaux.com/vls/v1/stations"
KAFKA_BROKER = "localhost:9092"
TOPIC = "velo_lyon_raw"
INTERVAL = 60  # secondes entre chaque appel

def create_producer():
    """
    Crée une connexion au broker Kafka.
    value_serializer : convertit automatiquement les dictionnaires Python
    en JSON encodé en UTF-8 avant de les envoyer dans Kafka.
    Sans ça, Kafka ne saurait pas comment transformer un dict Python en bytes.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_stations():
    """
    Appelle l'API JCDecaux et retourne la liste des stations.
    params : les paramètres de l'URL (contract=Lyon et la clé API)
    response.json() : convertit la réponse HTTP en liste de dictionnaires Python
    """
    params = {"contract": "Lyon", "apiKey": API_KEY}
    response = requests.get(API_URL, params=params)
    response.raise_for_status()  # lève une erreur si l'API retourne un code d'erreur (404, 500...)
    return response.json()

def main():
    """
    Boucle principale :
    1. Connecte au broker Kafka
    2. Appelle l'API toutes les 60 secondes
    3. Envoie chaque station comme un message dans le topic
    4. Recommence
    """
    print("Démarrage du producer Kafka...")
    print("Topic: {}".format(TOPIC))
    print("Broker: {}".format(KAFKA_BROKER))
    print("Intervalle: {}s".format(INTERVAL))
    print("-" * 40)

    producer = create_producer()

    while True:
        try:
            # Appel API
            stations = fetch_stations()
            timestamp = int(time.time())

            # Envoi de chaque station dans Kafka
            for station in stations:
                # On ajoute un timestamp de collecte pour savoir quand on a récupéré la donnée
                station["collected_at"] = timestamp
                producer.send(TOPIC, value=station)

            # flush() force l'envoi immédiat de tous les messages en attente
            # Sans ça, Kafka pourrait attendre d'avoir un lot de messages avant d'envoyer
            producer.flush()

            print("[{}] {} stations envoyées dans Kafka".format(
                time.strftime("%H:%M:%S"), len(stations)))

        except requests.exceptions.RequestException as e:
            # Si l'API ne répond pas (réseau, timeout...), on log l'erreur
            # et on réessaie au prochain cycle au lieu de crasher
            print("[{}] Erreur API: {}".format(time.strftime("%H:%M:%S"), e))

        except Exception as e:
            print("[{}] Erreur: {}".format(time.strftime("%H:%M:%S"), e))

        # Attente avant le prochain appel
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()