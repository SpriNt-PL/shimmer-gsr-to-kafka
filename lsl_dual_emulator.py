import time
import random
from pylsl import StreamInfo, StreamOutlet, local_clock

HZ = 128

def create_shimmer_outlet(sensor_name: str, source_id: str) -> StreamOutlet:
    # Creating channel with 3 streams
    info = StreamInfo(
        name=sensor_name,
        type='Biosignals',
        channel_count=3,
        nominal_srate=HZ,
        channel_format='float32',
        source_id=source_id
    )
    
    channels = info.desc().append_child("channels")
    
    ch_gsr = channels.append_child("channel")
    ch_gsr.append_child_value("label", "GSR_Skin_Conductance")
    ch_gsr.append_child_value("unit", "uS")
    ch_gsr.append_child_value("type", "GSR")
    
    ch_ppg = channels.append_child("channel")
    ch_ppg.append_child_value("label", "PPG_A1")
    ch_ppg.append_child_value("unit", "mV")
    ch_ppg.append_child_value("type", "PPG")

    ch_hr = channels.append_child("channel")
    ch_hr.append_child_value("label", "PPGtoHR")
    ch_hr.append_child_value("unit", "bpm")
    ch_hr.append_child_value("type", "HR")
    
    return StreamOutlet(info)

def main():
    outlet_1 = create_shimmer_outlet("Shimmer_ABA6", "shimmer_aba6_sim")
    outlet_2 = create_shimmer_outlet("Shimmer_87F3", "shimmer_87f3_sim")
    
    print("Broadcasting 3 simulated LSL streams: 'Shimmer_ABA6' and 'Shimmer_87F3'...")
    print("Press Ctrl+C to stop.")
    
    gsr_1, ppg_1, hr_1 = 5.90, 1410.0, 72.0
    gsr_2, ppg_2, hr_2 = 15.20, 2200.0, 80.0
    
    try:
        while True:
            start = time.perf_counter()
            now = local_clock()
            
            gsr_1 += random.uniform(-0.02, 0.02)
            ppg_1 += random.uniform(-1.5, 1.5)
            hr_1 += random.uniform(-0.1, 0.1)
            
            gsr_2 += random.uniform(-0.05, 0.05)
            ppg_2 += random.uniform(-2.0, 2.0)
            hr_2 += random.uniform(-0.15, 0.15)
            
            outlet_1.push_sample([gsr_1, ppg_1, hr_1], now)
            outlet_2.push_sample([gsr_2, ppg_2, hr_2], now)
            
            elapsed = time.perf_counter() - start
            time.sleep(max(0, (1.0 / HZ) - elapsed))
            
    except KeyboardInterrupt:
        print("\nStopped LSL dual emulator.")

if __name__ == "__main__":
    main()