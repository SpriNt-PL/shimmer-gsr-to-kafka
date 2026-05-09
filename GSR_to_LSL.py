#!/usr/bin/env python3

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

        # ---------------- GSR STREAM ----------------

        gsr_name = f"Shimmer_GSR_{self.com_port}"

        info_gsr = StreamInfo(
            gsr_name,
            'GSR',
            1,
            sample_rate,
            datatype,
            gsr_name
        )

        chns_gsr = info_gsr.desc().append_child("channels").append_child("channel")
        chns_gsr.append_child_value("label", "GSR")

        self.outlet_gsr = StreamOutlet(info_gsr)

        # ---------------- PPG STREAM ----------------

        ppg_name = f"Shimmer_PPG_{self.com_port}"

        info_ppg = StreamInfo(
            ppg_name,
            'PPG',
            1,
            sample_rate,
            datatype,
            ppg_name
        )

        chns_ppg = info_ppg.desc().append_child("channels").append_child("channel")
        chns_ppg.append_child_value("label", "PPG")

        self.outlet_ppg = StreamOutlet(info_ppg)

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

        # type(1) + ts(3) + GSR(2) + PPG(2)
        framesize = 8

        buffer = b""

        gsr_buffer = []
        ppg_buffer = []

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

                # ---------------- GSR CONVERSION ----------------

                rng = (gsr_raw >> 14) & 0x03

                rf = [40.2, 287.0, 1000.0, 3300.0][rng]

                volts = (gsr_raw & 0x3FFF) * (3.0 / 4095.0)

                gsr_ohm = rf / ((volts / 0.5) - 1.0)

                # ---------------- PPG CONVERSION ----------------

                ppg_mv = ppg_raw * (3000.0 / 4095.0)

                # ---------------- TIMESTAMP ----------------

                timestamp = time.time()

                # ---------------- BUFFER ----------------

                gsr_buffer.append([gsr_ohm])
                ppg_buffer.append([ppg_mv])

                # ---------------- PUSH CHUNK ----------------

                if len(gsr_buffer) >= self.chunk_size:

                    self.outlet_gsr.push_chunk(
                        gsr_buffer,
                        timestamp=timestamp
                    )

                    self.outlet_ppg.push_chunk(
                        ppg_buffer,
                        timestamp=timestamp
                    )

                    gsr_buffer = []
                    ppg_buffer = []

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