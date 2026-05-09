from pylsl import StreamInlet, resolve_byprop
import time

print("Scanning for LSL streams...")

# Find both streams
gsr_streams = resolve_byprop('type', 'GSR', timeout=5.0)
ppg_streams = resolve_byprop('type', 'PPG', timeout=5.0)

if not gsr_streams or not ppg_streams:
    print("Could not find both GSR and PPG streams. Make sure pyshimmer is running!")
else:
    inlet_gsr = StreamInlet(gsr_streams[0])
    inlet_ppg = StreamInlet(ppg_streams[0])
    print("--- Connected to both streams! ---")

    count = 0
    try:
        while True:
            # Pull one sample from each
            sample_gsr, _ = inlet_gsr.pull_sample()
            sample_ppg, _ = inlet_ppg.pull_sample()

            count += 1
            # Only print every 50th sample to prevent console tearing
            if count % 1 == 0:
                print(f"Live Data -> GSR: {sample_gsr[0]:.2f} kOhms  |  PPG: {sample_ppg[0]:.2f} mV")

    except KeyboardInterrupt:
        print("\nReceiver stopped.")