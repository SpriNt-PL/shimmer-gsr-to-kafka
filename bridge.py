# -*- coding: utf-8 -*-
"""
Created on DEC 11 10:01:18 2022

@author: schakraborth
"""
# Author: schakraborth@ethz.ch
# Tested on: Python 3.13
# Disabled the automatic scanning options for shimmer GSR. It was causing my algorithm to take too much time to connect and disconnect to the ports.

# before starting the main please fix shimmer ports. See the code in shimmerTrial.py

#%%

# 1 Required modules import
#--------------------
import threading
import sys
from Generator.GSR_to_LSL import GSR_PPG_to_LSL
import serial.tools.list_ports
#from ET_to_LSL import ET_to_LSL 

# At the current moment this works just with tobii.
# Update: Eye tracking at Winter School is kind of a great oppurtunity to extend this to gazepoint as well.

# 2 Main method
#------------------
# def main():
#     ports = serial.tools.list_ports.comports(include_links=False)
#     # 2.1 Set comport to GSR, ECG, and EEG
#     for port in ports:
#         print('Find port '+port.device )
#         # 2.2 Enable concurrent streaming of data
#         gsr = threading.Thread(target=GSR_PPG_to_LSL, args=(port.device, ))
#         gsr.start()
#     #et = threading.Thread(target=ET_to_LSL,args=(comX,))
#     #eeg = threading.Thread(target=EEG_to_LSL, args=(comUSB,))
#     # 2.3 Start concurrent running of streams
#     #et.start()
#     #eeg.start()

def main():
    # Bypass the auto-scanner and point directly to the virtual port
    port = 'COM10'
    print(f'Connecting directly to emulator on {port}...')

    gsr = threading.Thread(target=GSR_PPG_to_LSL, args=(port,))
    gsr.start()

    # Keep the main thread alive
    gsr.join()

if __name__ == "__main__":
    main()