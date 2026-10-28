import json
import time
from datetime import datetime
from confluent_kafka import Producer
from pylsl import StreamInlet, resolve_streams

# --- CONFIGURATION ---
PARTICIPATION_ID = "test_session_001"
KAFKA_SERVER = 'localhost:8081'  # SSH tunnel must be active!
TOPIC_NAME = 'biosignal-data'

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
channel_map = {}

for idx in range(info.channel_count()):
    label = ch_elem.child_value("label") or ch_elem.child_value("name") or f"Ch_{idx}"
    channel_map[label] = idx
    ch_elem = ch_elem.next_sibling()

# Target channel names based on Consensys LSL output
GSR_CHANNEL_NAME = 'GSR_Skin_Conductance'
PPG_CHANNEL_NAME = 'PPG_A1'

idx_gsr = channel_map.get(GSR_CHANNEL_NAME)
idx_ppg = channel_map.get(PPG_CHANNEL_NAME)

if idx_gsr is None or idx_ppg is None:
    print("Error: Required channels not found in the stream!")
    print(f"Available channels: {list(channel_map.keys())}")
    exit()

print(f"Mapping -> GSR ({GSR_CHANNEL_NAME}): index {idx_gsr} | PPG ({PPG_CHANNEL_NAME}): index {idx_ppg}")
print(f"--- Connected! Streaming to Kafka topic: {TOPIC_NAME} ---")

count = 0
try:
    while True:
        # Pull multi-channel sample with LSL timestamp
        sample, ts_lsl = inlet.pull_sample()

        if sample is None:
            continue

        count += 1
        ts_ms = int(ts_lsl * 1000)

        # Extract values for mapped channels
        gsr_val = float(sample[idx_gsr])
        ppg_val = float(sample[idx_ppg])

        payload = {
            "participation_id": PARTICIPATION_ID,
            # "ts_lsl": ts_lsl,        # Raw LSL timestamp
            "ts": ts_ms,               # Timestamp in ms
            "gsr": round(gsr_val, 2),  # GSR skin conductance value
            "ppg": round(ppg_val, 2),  # PPG raw ADC value
            "hr": 75.0                 # Hardcoded HR value (to be replaced with live calculation)
        }

        # Produce message to Kafka
        producer.produce(
            TOPIC_NAME,
            json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

        # Log every 50th sample to avoid terminal bottleneck
        if count % 50 == 0:
            print(f"[{ts_ms}] Sent to Kafka | GSR ({GSR_CHANNEL_NAME}): {payload['gsr']} | PPG: {payload['ppg']}")

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    producer.flush()