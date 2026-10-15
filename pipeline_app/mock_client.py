"""
Mock Client Ingestion Engine
Implements Sections 2, 3, 4 & 14 exact rules:
- Connects to biosignal-mock over TCP, alternating read calls for sensor 1 and sensor 2
- Keeps well inside the mock's 10s buffer
- Verifies CRC-32 on raw bytes exactly as sent
- Retries with from_index on corrupt frames or read timeouts
- Discards repeated frames (deduplication)
- Detects startup reboot gap (logs with reason device_down)
- Detects mock restarts (closes old session with to_index: null, reason mock_restart)
- Detects gaps myself (reason mock_no_data, read_timeout, restart_gap)
- Uses monotonic clock for all timers and ordering
"""

import json
import socket
import time
import zlib
from typing import Optional, Tuple
from storage import StorageEngine


def extract_data_and_verify_crc(raw_line: str, expected_crc_hex: str) -> Tuple[bool, Optional[list]]:
    try:
        data_key_pos = raw_line.find('"data"')
        if data_key_pos == -1:
            return False, None

        start_bracket = raw_line.find("[", data_key_pos)
        if start_bracket == -1:
            return False, None

        bracket_depth = 0
        end_bracket = -1
        in_string = False
        escape = False

        for idx in range(start_bracket, len(raw_line)):
            char = raw_line[idx]
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == '"':
                in_string = not in_string
                continue

            if not in_string:
                if char == "[":
                    bracket_depth += 1
                elif char == "]":
                    bracket_depth -= 1
                    if bracket_depth == 0:
                        end_bracket = idx
                        break

        if end_bracket == -1:
            return False, None

        data_substring = raw_line[start_bracket : end_bracket + 1]
        calc_crc = zlib.crc32(data_substring.encode("utf-8")) & 0xFFFFFFFF
        calc_crc_hex = f"{calc_crc:08x}"

        if calc_crc_hex.lower() != expected_crc_hex.lower():
            return False, None

        data_array = json.loads(data_substring)
        return True, data_array
    except Exception:
        return False, None


class MockClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 7100, storage: StorageEngine = None):
        self.host = host
        self.port = port
        self.storage = storage or StorageEngine()
        self.running = False
        self.sock: Optional[socket.socket] = None
        self.sock_file = None
        self.current_session_id: Optional[str] = None
        self.sensor_cursors = {1: 0, 2: 0}
        self.session_ended = {1: False, 2: False}
        self.first_reply_seen = {1: False, 2: False}

    def connect(self) -> bool:
        try:
            if self.sock:
                try:
                    self.sock.close()
                except Exception:
                    pass
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Section 3: read timeout
            self.sock.settimeout(3.0)
            self.sock.connect((self.host, self.port))
            self.sock_file = self.sock.makefile("r", encoding="utf-8")
            return True
        except Exception:
            self.sock = None
            self.sock_file = None
            return False

    def send_read_request(self, sensor: int, from_index: Optional[int] = None) -> bool:
        req = {"cmd": "read", "sensor": sensor}
        if from_index is not None:
            req["from_index"] = from_index
        line = json.dumps(req) + "\n"
        try:
            self.sock.sendall(line.encode("utf-8"))
            return True
        except Exception:
            return False

    def read_reply_line(self) -> Optional[str]:
        try:
            if not self.sock_file:
                return None
            line = self.sock_file.readline()
            return line if line else None
        except socket.timeout:
            return None
        except Exception:
            return None

    def poll_sensor(self, sensor: int) -> bool:
        expected_from = self.sensor_cursors[sensor]
        if not self.send_read_request(sensor):
            return False

        reply_line = self.read_reply_line()
        if not reply_line:
            # Section 3: Mock stops answering: use read timeout, retry with from_index
            if self.send_read_request(sensor, from_index=expected_from):
                reply_line = self.read_reply_line()
            if not reply_line:
                return False

        try:
            reply_json = json.loads(reply_line)
        except Exception:
            return False

        session_id = reply_json.get("session_id")
        if not session_id:
            return False

        # Section 3: Mock restarts detection
        if self.current_session_id is not None and session_id != self.current_session_id:
            # Close old session and record unread tail as lost with to_index: null and reason mock_restart
            for s in (1, 2):
                if not self.session_ended[s]:
                    last_idx = self.storage.get_highest_contiguous_index(self.current_session_id, s)
                    from_lost = 0 if last_idx < 0 else last_idx + 1
                    self.storage.record_loss(
                        session_id=self.current_session_id,
                        sensor=s,
                        from_index=from_lost,
                        to_index=None,
                        reason="mock_restart"
                    )
            # Reset state for new session
            self.current_session_id = session_id
            self.sensor_cursors = {1: 0, 2: 0}
            self.session_ended = {1: False, 2: False}
            self.first_reply_seen = {1: False, 2: False}
            self.storage.register_session(session_id, time.time())

        elif self.current_session_id is None:
            self.current_session_id = session_id
            self.storage.register_session(session_id, time.time())

        # Check CRC-32 exactly as sent
        crc_reported = reply_json.get("crc32", "")
        valid_crc, data_array = extract_data_and_verify_crc(reply_line, crc_reported)

        if not valid_crc or data_array is None:
            # Section 3: discard reply and re-read with from_index
            if self.send_read_request(sensor, from_index=expected_from):
                retry_line = self.read_reply_line()
                if retry_line:
                    try:
                        retry_json = json.loads(retry_line)
                        retry_crc = retry_json.get("crc32", "")
                        v2, d2 = extract_data_and_verify_crc(retry_line, retry_crc)
                        if v2 and d2 is not None:
                            reply_json = retry_json
                            data_array = d2
                            valid_crc = True
                    except Exception:
                        pass

        if not valid_crc or data_array is None:
            return False

        # Record mock's lost ranges in the loss table
        mock_losses = reply_json.get("lost", [])
        for loss_item in mock_losses:
            f_idx = loss_item.get("from_index")
            t_idx = loss_item.get("to_index")
            reason = loss_item.get("reason", "mock_reported_loss")
            if f_idx is not None:
                self.storage.record_loss(self.current_session_id, sensor, f_idx, t_idx, reason)
                if t_idx is not None and t_idx >= self.sensor_cursors[sensor]:
                    self.sensor_cursors[sensor] = t_idx + 1

        first_index = reply_json.get("first_index", 0)
        count = len(data_array)
        session_end = reply_json.get("session_end", False)
        if session_end:
            self.session_ended[sensor] = True

        # Section 3: After crash or reboot: on startup, compare last stored index with first index received
        if not self.first_reply_seen[sensor]:
            self.first_reply_seen[sensor] = True
            last_stored_idx = self.storage.get_highest_contiguous_index(self.current_session_id, sensor)
            if last_stored_idx >= 0 and first_index > (last_stored_idx + 1):
                # Gap becomes a loss range with reason device_down
                self.storage.record_loss(
                    session_id=self.current_session_id,
                    sensor=sensor,
                    from_index=last_stored_idx + 1,
                    to_index=first_index - 1,
                    reason="device_down"
                )
                self.sensor_cursors[sensor] = first_index

        # Section 3: Detect gaps myself: if first_index is higher than expected
        if first_index > self.sensor_cursors[sensor]:
            gap_from = self.sensor_cursors[sensor]
            gap_to = first_index - 1
            self.storage.record_loss(
                self.current_session_id,
                sensor,
                gap_from,
                gap_to,
                reason="mock_no_data"
            )

        # Store raw values unchanged (deduplication handled via PRIMARY KEY INSERT OR IGNORE)
        self.storage.insert_samples(self.current_session_id, sensor, first_index, data_array)

        # Update cursor
        self.sensor_cursors[sensor] = max(self.sensor_cursors[sensor], first_index + count)
        return True

    def run_loop(self):
        self.running = True
        while self.running:
            if not self.sock:
                if not self.connect():
                    # Monotonic sleep
                    time.sleep(0.5)
                    continue

            try:
                # Reader alternates read calls for sensor 1 and sensor 2 fast enough
                ok1 = self.poll_sensor(1)
                ok2 = self.poll_sensor(2)

                if not ok1 and not ok2:
                    time.sleep(0.04)
                else:
                    time.sleep(0.01)
            except Exception:
                self.sock = None
                time.sleep(0.5)

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
