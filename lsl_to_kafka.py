import json
import time
from datetime import datetime
from confluent_kafka import Producer
from pylsl import StreamInlet, resolve_byprop

# Configuration
PARTICIPATION_ID = "test_session_001"
KAFKA_SERVER = 'localhost:8081'  
TOPIC_NAME = 'biosignal-data'

producer_conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka Error: {err}")

print("Scanning for LSL streams...")

gsr_streams = resolve_byprop('type', 'GSR', timeout=5.0)
ppg_streams = resolve_byprop('type', 'PPG', timeout=5.0)

if not gsr_streams or not ppg_streams:
    print("Could not find both GSR and PPG streams.")
    exit()

inlet_gsr = StreamInlet(gsr_streams[0])
inlet_ppg = StreamInlet(ppg_streams[0])

print(f"Connected! Streaming to Kafka topic: {TOPIC_NAME}")

count = 0
try:
    while True:
        sample_gsr, ts_gsr = inlet_gsr.pull_sample()
        sample_ppg, ts_ppg = inlet_ppg.pull_sample()

        count += 1

        ts_ms = int(ts_gsr * 1000)
        
        human_date = datetime.fromtimestamp(ts_gsr).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]

        payload = {
            "participation_id": PARTICIPATION_ID,
            # "ts_lsl": ts_gsr,          # Raw time from LSL
            "ts": ts_ms,            # Timestamp in ms
            # "human_ts": human_date,    # Human readable date time
            "gsr": round(sample_gsr[0], 2), # GSR values 
            "ppg": round(sample_ppg[0], 2), # PPG values
            "hr": 75.0          # Hardcoded HR value (TO BE CHANGED LATER)
        }

        # Sending to Kafka
        producer.produce(
            TOPIC_NAME, 
            json.dumps(payload).encode('utf-8'), 
            callback=delivery_report
        )
        
        producer.poll(0)

        if count % 50 == 0:
            print(f"[{ts_ms}] Sent to Kafka | GSR: {payload['gsr']} | PPG: {payload['ppg']}")

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    producer.flush()