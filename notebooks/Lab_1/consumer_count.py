from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id = 'consumer_count',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. Zwiększ store_counts[store]
#   2. Dodaj amount do total_amount[store]
#   3. Co 10 wiadomości wypisz tabelę:
#      Sklep | Liczba | Suma | Średnia

for message in consumer:
    val = message.value
    store = val['store']
    amount = val['amount']
    
    store_counts[store] += 1
    
    if store not in total_amount:
        total_amount[store]=0.0

    total_amount[store] += amount

    msg_count += 1

    if msg_count % 10 == 0:
        for s in store_counts:
            liczba = store_counts[s]
            suma = total_amount[s]
            srednia = suma / liczba

            print(f"{s} | {liczba} | {suma:.2f} | {srednia:.2f}")
    
