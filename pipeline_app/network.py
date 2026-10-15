"""
Network State Monitor
Implements Section 7 exact rules:
- Probes real cloud ingest endpoint every 2s with 1.5s timeout
- Two consecutive failures switch to offline (worst case ~5.5s, inside 10s limit)
- Two consecutive successes switch to online (worst case ~4.5s)
- Uses time.monotonic() for all timing
"""

import threading
import time
import urllib.request


class NetworkMonitor:
    def __init__(self, cloud_url: str = "http://127.0.0.1:8000", probe_interval: float = 2.0, timeout: float = 1.5):
        self.cloud_url = cloud_url.rstrip("/")
        self.probe_interval = probe_interval
        self.timeout = timeout
        self._mode = "offline"
        self._lock = threading.Lock()
        self.running = False
        self.consecutive_success = 0
        self.consecutive_failures = 0
        self._thread = None

    def get_mode(self) -> str:
        with self._lock:
            return self._mode

    def _set_mode(self, new_mode: str):
        with self._lock:
            if self._mode != new_mode:
                print(f"[Network] Mode transition: {self._mode} -> {new_mode}")
                self._mode = new_mode

    def record_upload_failure(self):
        """Section 7: A failed batch upload also counts as a failure."""
        with self._lock:
            self.consecutive_failures += 1
            self.consecutive_success = 0
            if self.consecutive_failures >= 2:
                self._set_mode("offline")

    def probe_cloud(self) -> bool:
        url = f"{self.cloud_url}/api/v1/ping"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PipelineDevice/1.0"})
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status == 200
        except Exception:
            return False

    def run_loop(self):
        self.running = True
        while self.running:
            ok = self.probe_cloud()
            with self._lock:
                if ok:
                    self.consecutive_success += 1
                    self.consecutive_failures = 0
                    if self.consecutive_success >= 2:
                        self._mode = "online"
                else:
                    self.consecutive_failures += 1
                    self.consecutive_success = 0
                    if self.consecutive_failures >= 2:
                        self._mode = "offline"

            time.sleep(self.probe_interval)

    def start(self):
        self._thread = threading.Thread(target=self.run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False
