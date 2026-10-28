#!/usr/bin/env python3
# GSR_to_LSL.py

import struct
import argparse
import time
from pylsl import StreamInfo, StreamOutlet
from ShimmerCommands import ShimmerCommands


class GSR_PPG_to_LSL:

    def __init__(self, com_port, baud=115200, rate=512, chunk_size=1):
        self.com_port = com_port
        self.baud = baud
        self.rate = rate
        self.chunk_size = chunk_size

        self.ser = ShimmerCommands.serial_connect(self, com_port)

        self.setup_streams()

    def setup_streams(self):

        # Enable GSR + PPG sensors
        self.ser.write(struct.pack('BBBB', 0x08, 0x04, 0x01, 0x00))
        ShimmerCommands.wait_for_ack(self)
        print("Sensor setup (GSR+PPG) completed.")

        # Power on internal expansion board
        self.ser.write(struct.pack('BB', 0x5E, 0x01))
        ShimmerCommands.wait_for_ack(self)
        print("Internal expansion board power enabled.")

        sample_rate = self.rate
        datatype = 'float32'

        # --- JEDEN WIELOKANAŁOWY STRUMIEŃ (JAK W CONSENSYS PRO) ---
        stream_name = f"Shimmer_{self.com_port}"
        stream_type = "BioSignals"
        channel_count = 2

        info = StreamInfo(
            stream_name,
            stream_type,
            channel_count,
            sample_rate,
            datatype,
            f"shimmer_{self.com_port}"
        )

        channels = info.desc().append_child("channels")

        # Kanał 0: GSR (taka sama etykieta jak w Consensys)
        ch_gsr = channels.append_child("channel")
        ch_gsr.append_child_value("label", "GSR_Skin_Conductance")
        ch_gsr.append_child_value("unit", "kOhm")
        ch_gsr.append_child_value("type", "GSR")

        # Kanał 1: PPG (taka sama etykieta jak w Consensys)
        ch_ppg = channels.append_child("channel")
        ch_ppg.append_child_value("label", "PPG_A1")
        ch_ppg.append_child_value("unit", "mV")
        ch_ppg.append_child_value("type", "PPG")

        self.outlet = StreamOutlet(info)

        # Set sampling rate
        clock_wait = int((2 << 14) / sample_rate)

        print(clock_wait)

        self.ser.write(struct.pack('<BH', 0x05, clock_wait))
        ShimmerCommands.wait_for_ack(self)

        print(f"Sampling rate set to ~{sample_rate}Hz.")

        # Start streaming
        self.ser.write(struct.pack('B', 0x07))
        ShimmerCommands.wait_for_ack(self)

        print("Streaming started.")

        self.read_data_loop()

    def read_data_loop(self):

        framesize = 8
        buffer = b""

        sample_buffer = []

        print("Reading data... (Press Ctrl-C to stop)")

        try:
            while True:

                buffer += self.ser.read(framesize - len(buffer))

                if len(buffer) < framesize:
                    continue

                packet = buffer[:framesize]
                buffer = buffer[framesize:]

                _, t0, t1, t2, ppg_raw, gsr_raw = struct.unpack(
                    '<BBBBHH',
                    packet
                )

                # GSR conversion
                rng = (gsr_raw >> 14) & 0x03
                rf = [40.2, 287.0, 1000.0, 3300.0][rng]

                volts = (gsr_raw & 0x3FFF) * (3.0 / 4095.0)

                # Obliczenie oporu (zabezpieczenie dzielenia przez zero)
                denom = (volts / 0.5) - 1.0
                if abs(denom) > 1e-4:
                    gsr_val = rf / denom
                else:
                    gsr_val = rf

                # PPG conversion
                ppg_mv = ppg_raw * (3000.0 / 4095.0)

                # LSL timestamp
                timestamp = time.time()

                # Dodajemy obie wartości jako jedną 2-elementową próbkę
                sample_buffer.append([float(gsr_val), float(ppg_mv)])

                # Push chunk when full
                if len(sample_buffer) >= self.chunk_size:

                    self.outlet.push_chunk(
                        sample_buffer,
                        timestamp=timestamp
                    )

                    sample_buffer = []

        except KeyboardInterrupt:
            print("\nUser interrupted—Stopping stream...")

        except Exception as e:
            print(f"\nError occurred: {e}")

        finally:
            ShimmerCommands.stop_stream(self)
            print("Shimmer streaming stopped.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Stream Shimmer GSR & PPG to LSL"
    )

    parser.add_argument(
        "--port",
        required=True,
        help="COM port (e.g. COM5)"
    )

    parser.add_argument(
        "--baud",
        type=int,
        default=115200,
        help="Baud rate"
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=50,
        help="Sampling rate in Hz"
    )

    parser.add_argument(
        "--chunk",
        type=int,
        default=30,
        help="Chunk size"
    )

    args = parser.parse_args()

    GSR_PPG_to_LSL(
        args.port,
        args.baud,
        args.rate,
        args.chunk
    )