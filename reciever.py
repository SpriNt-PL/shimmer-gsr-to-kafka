import time
from pylsl import StreamInlet, resolve_streams

TARGET_CHANNELS = {
    "gsr": "GSR_Skin_Conductance",
    "ppg": "PPG_A1"
}

print("Scanning for Consensys/Shimmer LSL stream...")
streams = resolve_streams(wait_time=5.0)

if not streams:
    print("Could not find any active LSL streams.")
    exit()

shimmer_stream = streams[0]
inlet = StreamInlet(shimmer_stream)
info = inlet.info()

print(f"Connected to stream: '{info.name()}' ({info.channel_count()} channels)")

ch_elem = info.desc().child("channels").child("channel")
available_channels = {}

for idx in range(info.channel_count()):
    label = ch_elem.child_value("label") or ch_elem.child_value("name") or f"Ch_{idx}"
    available_channels[label] = idx
    ch_elem = ch_elem.next_sibling()

channel_indices = {}
missing_channels = []

for key, label in TARGET_CHANNELS.items():
    if label in available_channels:
        channel_indices[key] = (available_channels[label], label)
    else:
        missing_channels.append(label)

if missing_channels:
    print(f"Error: The following required channels were not found: {missing_channels}")
    print(f"Available channels: {list(available_channels.keys())}")
    exit()

print("Active channel mapping:")
for key, (idx, label) in channel_indices.items():
    print(f"  -> '{key}' <= '{label}' (index {idx})")

print("--- Listening for samples (Ctrl+C to stop) ---")

count = 0
try:
    while True:
        sample, ts_lsl = inlet.pull_sample()

        if sample is None:
            continue

        count += 1
        ts_ms = int(ts_lsl * 1000)

        gsr_val = float(sample[channel_indices["gsr"][0]])
        ppg_val = float(sample[channel_indices["ppg"][0]])

        print(f"Timestamp: {ts_ms} | GSR: {gsr_val:.2f} | PPG: {ppg_val:.2f}")

except KeyboardInterrupt:
    print("\nReceiver stopped.")