import json
import time
from datetime import datetime
from confluent_kafka import Producer
from pylsl import StreamInlet, resolve_byprop

# --- KONFIGURACJA ---
STUDENT_ID = "Kuba"
KAFKA_SERVER = 'localhost:8081'  # Tunel SSH musi być otwarty!
TOPIC_NAME = 'biosignal-data'      # Twój osobny topic

# Konfiguracja Producenta Kafki
producer_conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka Error: {err}")

# --- SZUKANIE STRUMIENI (TYLKO DWA, TAK JAK W TWOIM KODZIE) ---
print("Scanning for LSL streams...")

gsr_streams = resolve_byprop('type', 'GSR', timeout=5.0)
ppg_streams = resolve_byprop('type', 'PPG', timeout=5.0)

if not gsr_streams or not ppg_streams:
    print("Could not find both GSR and PPG streams.")
    exit()

inlet_gsr = StreamInlet(gsr_streams[0])
inlet_ppg = StreamInlet(ppg_streams[0])

print(f"--- Connected! Streaming to Kafka topic: {TOPIC_NAME} ---")

count = 0
try:
    while True:
        # Pobieranie próbek + LSL timestamp (tak jak w Twoim pliku)
        sample_gsr, ts_gsr = inlet_gsr.pull_sample()
        sample_ppg, ts_ppg = inlet_ppg.pull_sample()

        count += 1

        # Twoja logika timestampu w milisekundach
        ts_ms = int(ts_gsr * 1000)
        
        # Opcjonalnie: czytelna data do JSONa (na bazie LSL timestamp)
        # Uwaga: LSL timestamp często startuje od zera, więc human_ts może być dziwny, 
        # jeśli bridge nie synchronizuje zegara.
        human_date = datetime.fromtimestamp(ts_gsr).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]

        # Budujemy JSONa do wysyłki
        payload = {
            "student_id": STUDENT_ID,
            "ts_lsl": ts_gsr,          # Surowy czas z LSL
            "ts_ms": ts_ms,            # Twój czas w ms
            "human_ts": human_date,    # Czytelna data
            "gsr": round(sample_gsr[0], 2),
            "ppg": round(sample_ppg[0], 2)
        }

        # WYSYŁKA DO KAFKI
        producer.produce(
            TOPIC_NAME, 
            json.dumps(payload).encode('utf-8'), 
            callback=delivery_report
        )
        
        # Obsługa kolejki wysyłkowej
        producer.poll(0)

        # Wyświetlamy co 50-tą próbkę, żeby nie zapchać terminala (bottleneck!)
        if count % 50 == 0:
            print(f"[{ts_ms}] Sent to Kafka | GSR: {payload['gsr']} | PPG: {payload['ppg']}")

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    producer.flush()