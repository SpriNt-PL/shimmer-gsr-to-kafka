import time
from pylsl import StreamInlet, resolve_streams


def get_channel_metadata(inlet):
    """Extracts channel labels, units, and types from the LSL XML header."""
    info = inlet.info()
    ch = info.desc().child("channels").child("channel")
    channels = []

    for i in range(info.channel_count()):
        label = ch.child_value("label") or ch.child_value("name") or f"Ch_{i}"
        unit = ch.child_value("unit") or "no_unit"
        ch_type = ch.child_value("type") or "unknown"
        channels.append({"index": i, "label": label, "unit": unit, "type": ch_type})
        ch = ch.next_sibling()

    return channels


def main():
    print("Scanning network for ALL active LSL streams (waiting 5 seconds)...")
    streams = resolve_streams(wait_time=5.0)

    if not streams:
        print("No LSL streams detected! Make sure ConsensysPRO is streaming and LSL is checked.")
        return

    print(f"\nFound {len(streams)} stream(s):")
    print("=" * 75)

    inlets = []

    for idx, s in enumerate(streams):
        # Open inlet to fetch full XML metadata description
        inlet = StreamInlet(s)
        full_info = inlet.info()
        channels = get_channel_metadata(inlet)

        print(f"STREAM #{idx + 1}")
        print(f"  Name          : '{full_info.name()}'")
        print(f"  Type          : '{full_info.type()}'")
        print(f"  Source ID     : '{full_info.source_id()}'")
        print(f"  Sampling Rate : {full_info.nominal_srate()} Hz")
        print(f"  Channel Count : {full_info.channel_count()}")
        print(f"  Data Format   : {full_info.channel_format()}")

        print("  Channels inside packet:")
        for c in channels:
            print(f"    [{c['index']}] Label: '{c['label']}' | Unit: '{c['unit']}' | Type: '{c['type']}'")

        print("  --- Raw XML Header ---")
        print(full_info.as_xml().strip())
        print("=" * 75)

        inlets.append((full_info.name(), channels, inlet))

    print("\nListening for live packets (Press Ctrl+C to stop)...\n")

    try:
        while True:
            for stream_name, channels, inlet in inlets:
                # Non-blocking pull so we can check all streams in one loop
                sample, ts = inlet.pull_sample(timeout=0.0)
                if sample is not None:
                    # Pair each value in the packet list with its discovered channel label
                    paired_data = ", ".join(
                        f"{channels[i]['label']}: {val:.2f}"
                        for i, val in enumerate(sample)
                    )
                    print(f"[{stream_name} | ts: {ts:.3f}] -> {paired_data}")

            # Small sleep to prevent 100% CPU usage and terminal flooding
            time.sleep(0.05)

    except KeyboardInterrupt:
        print("\nSniffer stopped.")


if __name__ == "__main__":
    main()