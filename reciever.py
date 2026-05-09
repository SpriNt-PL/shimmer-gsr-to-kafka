from pylsl import StreamInlet, resolve_byprop

print("Scanning for LSL streams...")

# Szukamy naszego jednego, zbiorczego strumienia
streams = resolve_byprop('type', 'Biosignals', timeout=5.0)

if not streams:
    print("Could not find the Shimmer stream. Make sure pyshimmer is running!")
else:
    inlet = StreamInlet(streams[0])
    print("--- Connected to Shimmer stream! ---")

    count = 0
    try:
        while True:
            # LSL zwraca teraz listę 3 elementów: [GSR, PPG, Timestamp]
            sample, lsl_ts = inlet.pull_sample()

            count += 1
            # Wyświetlamy co 50. próbkę by nie zadławić bufora konsoli Windowsa
            if count % 50 == 0:
                gsr_val = sample[0]
                ppg_val = sample[1]
                hw_ts = sample[2]
                print(f"[{hw_ts:.3f}] Live Data -> GSR: {gsr_val:.2f} kOhms  |  PPG: {ppg_val:.2f} mV")

    except KeyboardInterrupt:
        print("\nReceiver stopped.")