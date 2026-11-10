import json
import time
from confluent_kafka import Producer
from pylsl import StreamInlet, resolve_streams

# Configuration
PARTICIPATION_ID = "test_session_001"
KAFKA_SERVER = 'localhost:8081'
TOPIC_NAME = 'biosignal-data'

# Defining channels
TARGET_CHANNELS = {
    "gsr": "GSR_Skin_Conductance",
    "ppg": "PPG_A1",
    # "gsr": "GSR",
    # "ppg": "PPG"
}

# Kafka Producer Configuration
producer_conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka Error: {err}")

# --- SCANNING FOR SHIMMER / CONSENSYS STREAM ---
print("Scanning for Consensys/Shimmer LSL stream...")
streams = resolve_streams(wait_time=5.0)

if not streams:
    print("No LSL streams detected! Ensure ConsensysPRO is actively streaming.")
    exit()

# Select the first detected stream (e.g. Shimmer_ABA6)
shimmer_stream = streams[0]
inlet = StreamInlet(shimmer_stream)
info = inlet.info()

print(f"Connected to stream: '{info.name()}' ({info.channel_count()} channels)")

# --- DYNAMIC CHANNEL MAPPING BY LABEL ---
ch_elem = info.desc().child("channels").child("channel")
available_channels = {}

for idx in range(info.channel_count()):
    label = ch_elem.child_value("label") or ch_elem.child_value("name") or f"Ch_{idx}"
    available_channels[label] = idx
    ch_elem = ch_elem.next_sibling()

channel_indices = {}
missing_channels = []

for json_key, lsl_label in TARGET_CHANNELS.items():
    if lsl_label in available_channels:
        channel_indices[json_key] = (available_channels[lsl_label], lsl_label)
    else:
        missing_channels.append(lsl_label)

if missing_channels:
    print(f"Error: The following required channels were not found in the stream: {missing_channels}")
    print(f"Available channels: {list(available_channels.keys())}")
    exit()

print("Active channel mapping:")
for json_key, (idx, lsl_label) in channel_indices.items():
    print(f"  -> JSON key '{json_key}' <= LSL channel '{lsl_label}' (index {idx})")

print(f"--- Connected! Streaming to Kafka topic: {TOPIC_NAME} ---")

count = 0
try:
    while True:
        sample, ts_lsl = inlet.pull_sample()

        if sample is None:
            continue

        count += 1
        ts_ms = int(ts_lsl * 1000)

        # Pola stałe
        payload = {
            "participation_id": PARTICIPATION_ID,
            "ts": ts_ms,
            "hr": 75.0  # Hardcoded HR value (to be replaced later)
        }

        for json_key, (idx, _) in channel_indices.items():
            payload[json_key] = round(float(sample[idx]), 2)

        # Produce message to Kafka
        producer.produce(
            TOPIC_NAME,
            json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

        if count % 50 == 0:
            signals_str = " | ".join(f"{k}: {payload[k]}" for k in channel_indices.keys())
            print(f"[{ts_ms}] Sent to Kafka | {signals_str}")

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    producer.flush()