import json
import time
import argparse
from confluent_kafka import Producer
from pylsl import StreamInlet, local_clock, resolve_byprop, resolve_streams

# Experiment configuration
PARTICIPATION_ID = "test_session_001"
SENSOR_NAME = "Shimmer_C1D2"

# Kafka Configuration
KAFKA_SERVER = 'localhost:8081'
TOPIC_NAME = 'biosignal-data'

# Defining channels
TARGET_CHANNELS = {
    "gsr": "GSR_Skin_Conductance",
    "ppg": "PPG_A1",
}

# Creating arguments parser
parser = argparse.ArgumentParser(description="Sending data from LSL to Kafka")

parser.add_argument("-test", "--test", action="store_true", help="Enable test mode")
args = parser.parse_args()

# Kafka Producer Configuration
producer = None
if not args.test:
    producer_conf = {'bootstrap.servers': KAFKA_SERVER}
    producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka Error: {err}")


# --- SCANNING FOR SPECIFIC SHIMMER STREAM BY NAME ---
print(f"Scanning network for LSL stream named '{SENSOR_NAME}'...")
streams = resolve_byprop('name', SENSOR_NAME, timeout=5.0)

if not streams:
    print(f"Error: Stream '{SENSOR_NAME}' not found!")
    all_streams = resolve_streams(wait_time=2.0)
    if all_streams:
        available_names = [s.name() for s in all_streams]
        print(f"Available LSL streams in network: {available_names}")
    else:
        print("No LSL streams detected at all. Ensure ConsensysPRO is streaming and firewall allows multicast.")
    exit()

shimmer_stream = streams[0]
inlet = StreamInlet(shimmer_stream)
info = inlet.info()

# Deriving the time offset (do synchronizacji z kamerą)
time_offset = time.time() - local_clock()

print(f"Connected to stream: '{info.name()}' ({info.channel_count()} channels, {info.nominal_srate()} Hz)")

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

if args.test:
    print(f"--- [TEST MODE] Connected! Reading '{SENSOR_NAME}' (Kafka disabled) ---")
else:
    print(f"--- Connected! Streaming '{SENSOR_NAME}' to Kafka topic: {TOPIC_NAME} ---")

count = 0
try:
    while True:
        sample, ts_lsl = inlet.pull_sample()

        if sample is None:
            continue

        count += 1

        ts_ms = int((ts_lsl + time_offset) * 1000)

        payload = {
            "participation_id": PARTICIPATION_ID,
            # "sensor_id": info.name(),
            "ts": ts_ms,
            "hr": 75.0  # Hardcoded HR value (to be replaced later !!!)
        }

        for json_key, (idx, _) in channel_indices.items():
            payload[json_key] = float(sample[idx])

        if not args.test:
            producer.produce(
                TOPIC_NAME,
                json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)

        if count % 50 == 0:
            signals_str = " | ".join(f"{k}: {payload[k]:.2f}" for k in channel_indices.keys())
            status_msg = "[TEST] Read" if args.test else "Sent to Kafka"
            print(f"[{info.name()} | {ts_ms}] {status_msg} | {signals_str}")

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    if producer is not None:
        producer.flush()