import json
import time
from datetime import datetime
from confluent_kafka import Producer
from pylsl import StreamInlet, resolve_streams

# --- KONFIGURACJA ---
STUDENT_ID = "Kuba" # Fix this as there was participation_id
KAFKA_SERVER = 'localhost:8081'  # Tunel SSH musi być otwarty!
TOPIC_NAME = 'biosignal-data'

producer_conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka Error: {err}")

# --- SZUKANIE STRUMIENIA SHIMMERA ---
print("Scanning for Consensys/Shimmer LSL stream...")
streams = resolve_streams(wait_time=5.0)

if not streams:
    print("Nie znaleziono strumieni LSL! Upewnij się, że ConsensysPRO nadaje.")
    exit()

# Wybieramy pierwszy znaleziony strumień (np. Shimmer_ABA6)
shimmer_stream = streams[0]
inlet = StreamInlet(shimmer_stream)
info = inlet.info()

print(f"Connected to stream: '{info.name()}' ({info.channel_count()} channels)")

# --- MAPOWANIE KANAŁÓW PO NAZWACH ---
ch_elem = info.desc().child("channels").child("channel")
channel_map = {}

for idx in range(info.channel_count()):
    label = ch_elem.child_value("label") or ch_elem.child_value("name") or f"Ch_{idx}"
    channel_map[label] = idx
    ch_elem = ch_elem.next_sibling()

# Wybór kanałów na podstawie logu ze sniffera:
# Możesz wybrać 'GSR_Skin_Conductance' lub 'GSR_Skin_Resistance'
GSR_CHANNEL_NAME = 'GSR_Skin_Conductance'
PPG_CHANNEL_NAME = 'PPG_A1'

idx_gsr = channel_map.get(GSR_CHANNEL_NAME)
idx_ppg = channel_map.get(PPG_CHANNEL_NAME)

if idx_gsr is None or idx_ppg is None:
    print(f"Błąd: Nie znaleziono wymaganych kanałów w strumieniu!")
    print(f"Dostępne kanały: {list(channel_map.keys())}")
    exit()

print(f"Mapping -> GSR ({GSR_CHANNEL_NAME}): index {idx_gsr} | PPG ({PPG_CHANNEL_NAME}): index {idx_ppg}")
print(f"--- Connected! Streaming to Kafka topic: {TOPIC_NAME} ---")

count = 0
try:
    while True:
        # Pobieramy próbkę z wszystkimi kanałami naraz
        sample, ts_lsl = inlet.pull_sample()

        if sample is None:
            continue

        count += 1
        ts_ms = int(ts_lsl * 1000)

        # Pobieramy konkretne wartości z wektora danych
        gsr_val = float(sample[idx_gsr])
        ppg_val = float(sample[idx_ppg])

        payload = {
            "student_id": STUDENT_ID,
            "ts": ts_ms,
            "gsr": round(gsr_val, 2),
            "ppg": round(ppg_val, 2),
            "hr": 75.0  # Wartość domyślna do czasu wyliczania HR
        }

        # WYSYŁKA DO KAFKI
        producer.produce(
            TOPIC_NAME,
            json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

        # Log co 50 próbek
        if count % 50 == 0:
            print(f"[{ts_ms}] Sent to Kafka | GSR ({GSR_CHANNEL_NAME}): {payload['gsr']} | PPG: {payload['ppg']}")

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    producer.flush()