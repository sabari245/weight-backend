# scale_reader.py
import serial
import logging
import time
import threading

# Import config selectively if needed, or pass params during init
# from config import SERIAL_TIMEOUT

class WeighingScale:
    """ Interfaces with the weighing scale device. """
    def __init__(self, port, baudrate=9600, timeout=1):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser = None
        self._buffer = ""
        # Lock might not be strictly needed if only one thread accesses instance
        # self._lock = threading.Lock()

    def connect(self):
        """ Opens the serial port connection. """
        if self.ser and self.ser.is_open:
            # logging.info(f"Serial port {self.port} already open.") # Maybe too verbose
            return True
        try:
            logging.info(f"Attempting to open serial port {self.port} at {self.baudrate} baud.")
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout,
            )
            logging.info(f"Serial port {self.port} opened successfully.")
            return True
        except serial.SerialException as e:
            logging.error(f"Error opening serial port {self.port}: {e}")
            self.ser = None
            return False

    def disconnect(self):
        """ Closes the serial port connection. """
        if self.ser and self.ser.is_open:
            try:
                self.ser.close()
                logging.info(f"Serial port {self.port} closed.")
            except Exception as e:
                logging.error(f"Error closing serial port {self.port}: {e}")
        self.ser = None

    def get_next_reading(self):
        """
        Reads from serial until a full STX/ETX message is received.
        Returns the processed weight (float) or None if no complete message or error.
        """
        if not self.ser or not self.ser.is_open:
            logging.warning("Serial port not open or available.")
            return None

        try:
            if self.ser.in_waiting > 0:
                try:
                    new_data = self.ser.read(self.ser.in_waiting).decode('ascii', errors='ignore')
                    self._buffer += new_data
                except serial.SerialException as e:
                    logging.error(f"Serial read error: {e}")
                    self.disconnect()
                    return None
                except Exception as e:
                    logging.error(f"Error decoding serial data: {e}")
                    return None

            while True:
                stx_index = self._buffer.find(chr(2)) # STX = \x02
                etx_index = self._buffer.find(chr(3)) # ETX = \x03

                if stx_index != -1 and etx_index != -1 and stx_index < etx_index:
                    raw_data = self._buffer[stx_index + 1 : etx_index]
                    self._buffer = self._buffer[etx_index + 1:]
                    logging.debug(f"Raw data received: {raw_data}")
                    weight = self._process_data(raw_data)
                    if weight is not None:
                        return weight
                    # else continue loop
                elif stx_index != -1 and etx_index == -1 and len(self._buffer) > 500: # Heuristic Limit
                     logging.warning(f"Buffer growing large without ETX, clearing part of it.")
                     last_stx = self._buffer.rfind(chr(2))
                     self._buffer = self._buffer[last_stx:] if last_stx != -1 else ""
                     return None
                elif stx_index == -1 and len(self._buffer) > 256: # Heuristic Limit
                     logging.warning(f"No STX found in large buffer, discarding.")
                     self._buffer = ""
                     return None
                else:
                    return None # Wait for more data

        except Exception as e:
            logging.exception(f"Unexpected error in get_next_reading: {e}")
            return None

    def _process_data(self, data):
        """
        Processes the raw data string (between STX/ETX) to extract the weight.
        Returns float weight or None if processing fails.
        !!! ADJUST THIS BASED ON YOUR *ACTUAL* SCALE'S OUTPUT FORMAT !!!
        """
        try:
            if len(data) < 8:
                 logging.warning(f"Data length insufficient for processing: '{data}'")
                 return None

            weight_chars = list(data[:7])
            decimal_byte = data[7]
            decimal_pos_indicator = ord(decimal_byte) - ord('0')

            if not 0 <= decimal_pos_indicator <= 7:
                logging.warning(f"Invalid decimal position indicator '{decimal_byte}' in data: '{data}'")
                return None

            decimal_position_index = 7 - decimal_pos_indicator
            if decimal_position_index < 7:
                 weight_chars.insert(decimal_position_index, '.')
            weight_str = "".join(weight_chars).strip()
            return float(weight_str)

        except (ValueError, IndexError, TypeError) as e:
            logging.error(f"Error processing data: {e}. Data received: '{data}'")
            return None