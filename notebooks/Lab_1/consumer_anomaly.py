from kafka import KafkaConsumer 
import json 
from datetime import datetime

consumer = KafkaConsumer( 
    'transactions', #nazwa topic 
    bootstrap_servers='broker:9092', 
    group_id = 'consumer_anomaly',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) #deserializacja i loads, zamieniamy na slownik 
) 


# TWÓJ KOD 
# Napisz konsumenta wykrywającego anomalie prędkości: alert jeśli ten
# sam user_id wykona więcej niż 3 transakcje w ciągu 60 sekund.

user_history = {}

for message in consumer: 
    val = message.value
    user = val['user_id']
    transaction_time = datetime.fromisoformat(val['timestamp'])

    if user not in user_history:
        user_history[user] = []

    user_history[user].append(transaction_time)

    recent_transactions = []
    
    for saved_time in user_history[user]:
        time_diff = (transaction_time - saved_time).total_seconds()
        if time_diff <= 60.0:
            recent_transactions.append(saved_time)

    user_history[user] = recent_transactions

    if len(user_history[user]) > 3:
        print(f"ALERT: User {user} wykonał {len(user_history[user])} transakcji w ciągu ostatnich 60 sekund.")

    
