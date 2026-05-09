import socket
import struct
import time
import random

# CONFIGURATION
TCP_IP = '127.0.0.1'
TCP_PORT = 10001
HZ = 512

state = {
    "gsr": [1257.0, 1200.0, 0.01, 0.2],
    "ppg": [1180.0, 1150.0, 0.05, 1.5]
}

def generate_organic_values():
    out = {}
    for key in ["gsr", "ppg"]:
        current, target, k, noise_lvl = state[key]
        pull = k * (target - current)
        jitter = random.uniform(-noise_lvl, noise_lvl)
        new_val = current + pull + jitter
        state[key][0] = new_val
        out[key] = new_val
    return out

def pack_gsr_internal(kohm):
    if kohm <= 0: kohm = 0.1
    volts = 0.5 * ((287.0 / kohm) + 1.0)
    adc_val = int(volts * (4095.0 / 3.0)) & 0x3FFF
    return (1 << 14) | adc_val

def pack_ppg_internal(mv):
    return int(mv * (4095.0 / 3000.0)) & 0x0FFF

def run_emulator():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Wyłączenie algorytmu Nagle'a - natychmiastowa wysyłka bez buforowania TCP
    server_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    try:
        server_sock.bind((TCP_IP, TCP_PORT))
        server_sock.listen(1)
        print(f"Emulator ready at {HZ}Hz. Waiting for HW VSP3 on port {TCP_PORT}...")

        conn, addr = server_sock.accept()
        print("HW VSP3 Connected! Broadcasting to COM10.")

        streaming = False
        valid_commands = [b'\x08', b'\x5E', b'\x05', b'\x07', b'\x20']

        while True:
            conn.setblocking(False)
            try:
                cmd = conn.recv(1)
                if cmd:
                    if cmd in valid_commands:
                        conn.sendall(b'\xff')
                        if cmd == b'\x07':
                            print("Stream Started!")
                            streaming = True
                        elif cmd == b'\x20':
                            print("Stream Stopped!")
                            streaming = False
            except BlockingIOError:
                pass

            if streaming:
                start_loop = time.perf_counter()

                organic_data = generate_organic_values()
                gsr_bits = pack_gsr_internal(organic_data['gsr'])
                ppg_bits = pack_ppg_internal(organic_data['ppg'])

                ts_ms = int(time.time() * 1000) & 0xFFFFFF
                t0 = ts_ms & 0xFF
                t1 = (ts_ms >> 8) & 0xFF
                t2 = (ts_ms >> 16) & 0xFF

                packet = struct.pack('<BBBBHH', 0x00, t0, t1, t2, ppg_bits, gsr_bits)
                conn.send(packet)

                # Spin-lock o wysokiej precyzji dla utrzymania dokładnych 512 Hz
                elapsed = time.perf_counter() - start_loop
                sleep_time = (1.0 / HZ) - elapsed
                if sleep_time > 0:
                    target_time = time.perf_counter() + sleep_time
                    while time.perf_counter() < target_time:
                        pass
            else:
                time.sleep(0.01)

    except Exception as e:
        print(f"Network Error: {e}")
    finally:
        server_sock.close()

if __name__ == "__main__":
    run_emulator()