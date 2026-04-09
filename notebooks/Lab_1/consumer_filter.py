from kafka import KafkaConsumer 
import json 

consumer = KafkaConsumer( 
    'transactions', #nazwa topic 
    bootstrap_servers='broker:9092', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
) 

#for message in consumer: 
#    print(message) 

# TWÓJ KOD 
# Dla każdej wiadomości: sprawdź czy amount > 3000, jeśli tak — wypisz ALERT
# Format: ALERT: TX0042 | 2345.67 PLN | Warszawa | elektronika

for message in consumer: 
    val = message.value
    if val['amount']>3000:
        print(f"ALERT: {val['tx_id']} | {val['amount']} PLN | {val['store']} | {val['category']}")
    
