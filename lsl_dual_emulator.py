import time
import random
from pylsl import StreamInfo, StreamOutlet, local_clock

HZ = 128  # Taka sama częstotliwość jak w Twoim logu z prawdziwego Shimmera

def create_shimmer_outlet(sensor_name: str, source_id: str) -> StreamOutlet:
    # Tworzymy strumień z 2 kanałami o dokładnych nazwach oczekiwanych przez lsl_to_kafka.py
    info = StreamInfo(
        name=sensor_name,
        type='Biosignals',
        channel_count=2,
        nominal_srate=HZ,
        channel_format='float32',
        source_id=source_id
    )
    
    # Dodajemy metadane XML kanałów (tak jak robi to ConsensysPRO)
    channels = info.desc().append_child("channels")
    
    ch_gsr = channels.append_child("channel")
    ch_gsr.append_child_value("label", "GSR_Skin_Conductance")
    ch_gsr.append_child_value("unit", "uS")
    ch_gsr.append_child_value("type", "GSR")
    
    ch_ppg = channels.append_child("channel")
    ch_ppg.append_child_value("label", "PPG_A1")
    ch_ppg.append_child_value("unit", "mV")
    ch_ppg.append_child_value("type", "PPG")
    
    return StreamOutlet(info)

def main():
    outlet_1 = create_shimmer_outlet("Shimmer_ABA6", "shimmer_aba6_sim")
    outlet_2 = create_shimmer_outlet("Shimmer_C1D2", "shimmer_c1d2_sim")
    
    print("Broadcasting 2 simulated LSL streams: 'Shimmer_ABA6' and 'Shimmer_C1D2'...")
    print("Press Ctrl+C to stop.")
    
    # Różne zakresy wartości, żebyś w logach Kafki od razu widział, z którego sensora pochodzą dane
    gsr_1, ppg_1 = 5.90, 1410.0   # Shimmer_ABA6 (~6 uS, ~1410 mV)
    gsr_2, ppg_2 = 15.20, 2200.0  # Shimmer_C1D2 (~15 uS, ~2200 mV)
    
    try:
        while True:
            start = time.perf_counter()
            now = local_clock()
            
            gsr_1 += random.uniform(-0.02, 0.02)
            ppg_1 += random.uniform(-1.5, 1.5)
            
            gsr_2 += random.uniform(-0.05, 0.05)
            ppg_2 += random.uniform(-2.0, 2.0)
            
            outlet_1.push_sample([gsr_1, ppg_1], now)
            outlet_2.push_sample([gsr_2, ppg_2], now)
            
            elapsed = time.perf_counter() - start
            time.sleep(max(0, (1.0 / HZ) - elapsed))
            
    except KeyboardInterrupt:
        print("\nStopped LSL dual emulator.")

if __name__ == "__main__":
    main()