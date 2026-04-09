from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id = 'consumer_stats',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

category_counts = Counter()
total_amount = {}
min_amount = {}
max_amount = {}
msg_count = 0

# TWÓJ KOD
for message in consumer:
    val = message.value
    category = val['category']
    amount = val['amount']
    
    category_counts[category] += 1
    
    if category not in total_amount:
        total_amount[category]=0.0
        min_amount[category] = amount
        max_amount[category] = amount

    total_amount[category] += amount
    min_amount[category] = min(min_amount[category], amount)
    max_amount[category] = max(max_amount[category], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        for c in category_counts:
            liczba = category_counts[c]
            suma = total_amount[c]
            minimum = min_amount[c]
            maximum = max_amount[c]

            print(f"{c} | {liczba} | {suma:.2f} | {minimum} | {maximum}")
    
