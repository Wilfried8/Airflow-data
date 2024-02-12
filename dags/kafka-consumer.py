from kafka import KafkaConsumer

# Configuration du serveur Kafka et du topic
bootstrap_servers = 'broker:29092'
topic = 'users_created'

# Création d'un consummer Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id='my_consumer_group',  # Définir un groupe de consumer pour suivre la lecture
    auto_offset_reset='earliest',  # Commencer à lire à partir du plus ancien message non lue
    enable_auto_commit=True,  # Activer la confirmation automatique de l'engagement
    auto_commit_interval_ms=1000,  # Intervalle de confirmation automatique de l'engagement
    value_deserializer=lambda x: x.decode('utf-8')  # Désérialiser la valeur du message comme une chaîne de caractères
)

# Boucle de consommation des messages
for message in consumer:
    print(f"Key: {message.key}, Value: {message.value}")
