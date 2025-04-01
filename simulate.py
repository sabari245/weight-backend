import time
import serial
import random

port = "/dev/pts/6"  # The second virtual port
ser = serial.Serial(port, baudrate=9600, timeout=1)

while True:
    weight = random.randint(10000000, 99999999)  # Generate a random 8-digit weight
    weight_str = str(weight)
    ser.write(b"\x02" + weight_str.encode() + b"3" + b"\x03")  # Example weight format
    time.sleep(3)  # Simulate real-time streaming