from pylsl import StreamInlet, resolve_byprop

print("Scanning for LSL streams...")

# Find streams

gsr_streams = resolve_byprop('type', 'GSR', timeout=5.0)
ppg_streams = resolve_byprop('type', 'PPG', timeout=5.0)

if not gsr_streams or not ppg_streams:

    print("Could not find both GSR and PPG streams.")

else:

    inlet_gsr = StreamInlet(gsr_streams[0])
    inlet_ppg = StreamInlet(ppg_streams[0])

    print("--- Connected to both streams! ---")

    count = 0

    try:

        while True:

            # sample + LSL timestamp

            sample_gsr, ts_gsr = inlet_gsr.pull_sample()
            sample_ppg, ts_ppg = inlet_ppg.pull_sample()

            count += 1

            # milliseconds timestamp
            ts_ms = int(ts_gsr * 1000)

            print(
                f"Timestamp: {ts_ms} | "
                f"GSR: {sample_gsr[0]:.2f} kOhms | "
                f"PPG: {sample_ppg[0]:.2f} mV"
            )

    except KeyboardInterrupt:

        print("\nReceiver stopped.")