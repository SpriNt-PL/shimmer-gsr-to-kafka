# Order of execution:

## Using the generator: 
1. Turn on HW Virtual Serial Port and create COM
2. Run generator.py
3. Run GSR_to_LSL.py
4. Run lsl_to_kafka_2.py / reciever.py

## Using the Shimmer sensor: 
1. Turn on Shimmer sensor
2. Connect sensor in the Consensys PRO
3. Make sure the stream has started
4. Run lsl_to_kafka_2.py / reciever.py

##
You might need to set up "lsl_api.cfg" file to recive the stream in case of streaming from the other device.
Paste the following code:
```
[lab]
KnownPeers = {REMOTE_DEVICE_IP}
```