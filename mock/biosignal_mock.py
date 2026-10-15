"""
Biosignal Mock Service
Implements Section 6 of the assessment specification:
- Realistic synthetic ECG/PPG ("--mode signal") and deterministic ramp ("--mode ramp")
- TCP newline-delimited JSON server
- CRC32 calculation over raw data text
- 10-second ring buffer with overflow loss reporting
- Fault injection capabilities for test validation (crc corruption, duplicates, timeouts, restarts)
"""

import argparse
import json
import math
import random
import socket
import sys
import threading
import time
import zlib


def compute_crc32_hex(data_str: str) -> str:
    """CRC-32 (zlib) of data text exactly from [ to the matching ]."""
    crc = zlib.crc32(data_str.encode("utf-8")) & 0xFFFFFFFF
    return f"{crc:08x}"


class BiosignalGenerator:
    def __init__(self, mode: str = "signal", seed: int = 7, duration: int = 3600):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.sample_rate = 400
        self.total_samples = duration * self.sample_rate
        self.rng = random.Random(seed)

        # Precompute beat intervals for mode signal
        # HR is 60 bpm for min 0-2 (0-120s), 90 for min 2-4 (120-240s), 72 for min 4-6 (240-360s), repeating every 360s
        self._peaks_s1 = set()
        if self.mode == "signal":
            self._precompute_signal_peaks()

    def _precompute_signal_peaks(self):
        """Generates physiological ECG peak positions across duration."""
        current_sample = 200  # initial offset
        while current_sample < self.total_samples:
            sec = current_sample / self.sample_rate
            cycle_sec = sec % 360.0
            if cycle_sec < 120.0:
                bpm = 60.0
            elif cycle_sec < 240.0:
                bpm = 90.0
            else:
                bpm = 72.0

            gap_samples = int(round(self.sample_rate * (60.0 / bpm)))
            self._peaks_s1.add(current_sample)
            current_sample += gap_samples

    def get_sample(self, sensor: int, index: int) -> list:
        """Returns values for sample `index` of `sensor`."""
        if index < 0 or index >= self.total_samples:
            return None

        if self.mode == "ramp":
            if sensor == 1:
                return [(3 * index + c) % 8388608 for c in range(3)]
            else:
                return [(2 * index + c) % 8388608 for c in range(2)]

        # Mode signal
        if sensor == 1:
            # Channel 2 is ECG
            # Baseline ~ 1000 with cardiac wave
            base = 1000
            # Check proximity to a peak
            val_ch2 = base
            # Find nearest peak
            is_peak = index in self._peaks_s1
            if is_peak:
                val_ch2 = 2500  # sharp R-peak
            elif (index - 1) in self._peaks_s1 or (index + 1) in self._peaks_s1:
                val_ch2 = 1400  # slopes
            elif (index - 2) in self._peaks_s1 or (index + 2) in self._peaks_s1:
                val_ch2 = 800   # Q / S dip
            else:
                # Slight baseline wander and small T wave
                t = index / self.sample_rate
                val_ch2 = int(base + 50 * math.sin(2 * math.pi * 0.2 * t))

            ch1 = int(round(val_ch2 * 0.6))
            ch2 = int(val_ch2)
            ch3 = int(round(val_ch2 * 1.3))
            return [ch1, ch2, ch3]
        else:
            # Sensor 2: channel 1 near 400,000, channel 2 near 150,000, varying slowly
            t = index / self.sample_rate
            ch1 = int(400000 + 5000 * math.sin(2 * math.pi * 0.05 * t))
            ch2 = int(150000 + 3000 * math.cos(2 * math.pi * 0.03 * t))
            return [ch1, ch2]


class MockServer:
    def __init__(self, mode="signal", seed=7, duration=3600, port=7100, session_id=None):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.session_id = session_id or f"{random.randint(0, 0xFFFFFFFF):08x}"
        self.generator = BiosignalGenerator(mode=mode, seed=seed, duration=duration)
        self.sample_rate = 400
        self.start_time = None
        self.running = False
        self.server_socket = None

        # Buffer: keeps last 10 seconds (4000 samples)
        self.buffer_size = 10 * self.sample_rate
        # Per sensor cursor tracking
        self.sensor_cursors = {1: 0, 2: 0}

        # Fault injection hooks
        self.inject_crc_error = False
        self.inject_duplicate = False
        self.inject_stall_sec = 0
        self.inject_drop_sensor_1 = False

    def current_produced_index(self) -> int:
        if self.start_time is None:
            return 0
        elapsed = time.time() - self.start_time
        return min(int(elapsed * self.sample_rate), self.generator.total_samples)

    def handle_read(self, sensor: int, from_index: int = None) -> dict:
        now_idx = self.current_produced_index()
        req_from = self.sensor_cursors[sensor] if from_index is None else from_index

        lost = []
        oldest_buffered = max(0, now_idx - self.buffer_size)

        if req_from < oldest_buffered:
            # Buffer overflow! Unread data was lost
            loss_to = oldest_buffered - 1
            lost.append({
                "from_index": req_from,
                "to_index": loss_to,
                "reason": "buffer_overflow"
            })
            req_from = oldest_buffered

        # Can return at most 1 second of data (400 samples)
        count = min(400, max(0, now_idx - req_from))
        data = []
        for i in range(req_from, req_from + count):
            s = self.generator.get_sample(sensor, i)
            if s is not None:
                data.append(s)

        # Update cursor
        self.sensor_cursors[sensor] = req_from + count
        session_end = (req_from + count >= self.generator.total_samples)

        # Construct JSON data string directly to compute exact CRC-32
        data_json = json.dumps(data, separators=(",", " "))
        crc = compute_crc32_hex(data_json)

        if self.inject_crc_error:
            self.inject_crc_error = False
            crc = "deadbeef"  # intentionally corrupt

        reply = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": req_from,
            "count": len(data),
            "lost": lost,
            "session_end": session_end,
            "crc32": crc,
            "data": data,
            "_raw_data_json": data_json
        }
        return reply

    def start(self):
        self.running = True
        self.start_time = time.time()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("0.0.0.0", self.port))
        self.server_socket.listen(1)
        print(f"[Mock] Running on port {self.port}, mode={self.mode}, seed={self.seed}, session={self.session_id}")

        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                print(f"[Mock] Accepted connection from {addr}")
                self._handle_client(client_sock)
            except Exception as e:
                if self.running:
                    print(f"[Mock] Accept error: {e}")
                break

    def _handle_client(self, client_sock):
        client_sock.settimeout(5.0)
        buffer = ""
        try:
            while self.running:
                data = client_sock.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        req = json.loads(line)
                    except Exception as e:
                        print(f"[Mock] Invalid JSON request: {line}")
                        continue

                    if req.get("cmd") == "read":
                        sensor = req.get("sensor", 1)
                        from_idx = req.get("from_index", None)

                        if self.inject_stall_sec > 0:
                            time.sleep(self.inject_stall_sec)
                            self.inject_stall_sec = 0

                        reply_dict = self.handle_read(sensor, from_idx)
                        raw_data_str = reply_dict.pop("_raw_data_json")

                        # Reconstruct line to guarantee CRC matches data array formatting
                        msg_obj = {
                            "session_id": reply_dict["session_id"],
                            "sensor": reply_dict["sensor"],
                            "first_index": reply_dict["first_index"],
                            "count": reply_dict["count"],
                            "lost": reply_dict["lost"],
                            "session_end": reply_dict["session_end"],
                            "crc32": reply_dict["crc32"],
                        }
                        prefix = json.dumps(msg_obj)[:-1]  # drop trailing '}'
                        line_out = f'{prefix}, "data": {raw_data_str}}}\n'

                        client_sock.sendall(line_out.encode("utf-8"))

                        if self.inject_duplicate:
                            self.inject_duplicate = False
                            client_sock.sendall(line_out.encode("utf-8"))

        except Exception as e:
            # Client disconnected or timeout
            pass
        finally:
            client_sock.close()

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Biosignal Mock Service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--session-id", type=str, default=None)
    args = parser.parse_args()

    mock = MockServer(mode=args.mode, seed=args.seed, duration=args.duration, port=args.port, session_id=args.session_id)
    try:
        mock.start()
    except KeyboardInterrupt:
        print("\nStopping mock server...")
        mock.stop()


if __name__ == "__main__":
    main()
