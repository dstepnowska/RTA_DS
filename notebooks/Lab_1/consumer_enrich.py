from kafka import KafkaConsumer
import json

# TWÓJ KOD
# Czytaj z 'transactions' (użyj INNEGO group_id!)
# Dodaj pole risk_level na podstawie amount
# Wypisz wzbogaconą transakcję

consumer = KafkaConsumer( 
    'transactions', #nazwa topic 
    bootstrap_servers='broker:9092',
    group_id='consumer_enrich',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) #deserializacja i loads, zamieniamy na slownik 
) 

for message in consumer: #dostajemy sie do slownika 
    val = message.value
    if val['amount'] > 3000:
        val['risk_level'] = 'HIGH'
    elif val['amount'] > 1000:
        val['risk_level'] = 'MEDIUM'
    else:
        val['risk_level'] = 'LOW'

    print(val)        

