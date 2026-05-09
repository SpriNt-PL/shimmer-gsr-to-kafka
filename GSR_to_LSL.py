#!/usr/bin/env python3
import struct
import argparse
from pylsl import StreamInfo, StreamOutlet
from ShimmerCommands import ShimmerCommands
import time

class GSR_PPG_to_LSL:
    def __init__(self, com_port, baud=115200, rate=512, chunk_size=32):
        self.com_port = com_port
        self.baud = baud
        self.rate = rate
        self.chunk_size = chunk_size
        self.start_unix_time = time.time()
        self.first_hw_ts = None
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

        # Create SINGLE LSL outlet for all 3 channels
        sample_rate = self.rate
        datatype = 'double64' # Wymagane, by nie stracić precyzji Timestampu!
        stream_name = f"Shimmer_Data_{self.com_port}"

        info = StreamInfo(stream_name, 'Biosignals', 3, sample_rate, datatype, stream_name)
        chns = info.desc().append_child("channels")
        
        for label in ["GSR", "PPG", "HW_Timestamp"]:
            chns.append_child("channel").append_child_value("label", label)
            
        self.outlet = StreamOutlet(info)

        # Set sampling rate
        clock_wait = int((2 << 14) / sample_rate)
        print(f"Clock wait set to: {clock_wait}")
        self.ser.write(struct.pack('<BH', 0x05, clock_wait))
        ShimmerCommands.wait_for_ack(self)
        print(f"Sampling rate set to ~{sample_rate}Hz.")

        # Start streaming
        self.ser.write(struct.pack('B', 0x07))
        ShimmerCommands.wait_for_ack(self)
        print("Streaming started.")

        self.read_data_loop()

    def read_data_loop(self):
        framesize = 8  # Packet: type(1) + ts(3) + GSR(2) + PPG(2)
        buffer = b""
        
        # Jeden wspólny bufor dla wszystkich danych
        chunk_buffer = []

        print("Reading data... (Press Ctrl-C to stop)")

        try:
            while True:
                buffer += self.ser.read(framesize - len(buffer))
                if len(buffer) < framesize:
                    continue

                packet = buffer[:framesize]
                buffer = buffer[framesize:]

                _, t0, t1, t2, ppg_raw, gsr_raw = struct.unpack('<BBBBHH', packet)
                hardware_ts = t0 + (t1 << 8) + (t2 << 16)

                if self.first_hw_ts is None:
                    self.first_hw_ts = hardware_ts

                time_diff_sec = (hardware_ts - self.first_hw_ts) / 1000.0

                if time_diff_sec < 0: 
                    time_diff_sec += (16777216 / 1000.0)

                full_unix_ts = self.start_unix_time + time_diff_sec

                # GSR conversion
                rng = (gsr_raw >> 14) & 0x03
                rf = [40.2, 287.0, 1000.0, 3300.0][rng]
                volts = (gsr_raw & 0x3FFF) * (3.0 / 4095.0)
                gsr_ohm = rf / ((volts / 0.5) - 1.0) if ((volts / 0.5) - 1.0) != 0 else 0.1

                # PPG conversion
                ppg_mv = ppg_raw * (3000.0 / 4095.0)

                # Add [GSR, PPG, TS] as a single sample to the chunk
                chunk_buffer.append([gsr_ohm, ppg_mv, full_unix_ts])

                # Push chunk when full
                if len(chunk_buffer) >= self.chunk_size:
                    self.outlet.push_chunk(chunk_buffer)
                    chunk_buffer = []

        except KeyboardInterrupt:
            print("\nUser interrupted—Stopping stream...")
        except Exception as e:
            print(f"\nError occurred: {e}")
        finally:
            ShimmerCommands.stop_stream(self)
            print("Shimmer streaming stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream Shimmer GSR & PPG to LSL (chunked)")
    parser.add_argument("--port",  required=True, help="COM port (e.g. COM5)")
    parser.add_argument("--baud",  type=int, default=115200, help="Baud rate (default: 115200)")
    parser.add_argument("--rate",  type=int, default=512, help="Sampling rate in Hz (default: 512)")
    parser.add_argument("--chunk", type=int, default=32, help="Number of samples per chunk (default: 32)")
    args = parser.parse_args()

    GSR_PPG_to_LSL(args.port, args.baud, args.rate, args.chunk)