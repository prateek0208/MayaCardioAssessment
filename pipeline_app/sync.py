"""
Cloud Synchronization & Catch-up Worker
Implements Sections 6, 7, 8 & 14:
- Continuous small batch upload (~1 second) during online mode over HTTP/HTTPS with gzip
- Catches up after outage with batches up to 5,000 samples, oldest first
- Synchronizes lost ranges so cloud export matches device exactly
- Synchronizes offline window results so cloud maintains complete copy of all windows
- Pauses cleanly during offline mode
"""

import gzip
import json
import threading
import time
import urllib.request
from typing import Optional
from network import NetworkMonitor
from storage import StorageEngine


class SyncWorker:
    def __init__(self, storage: StorageEngine, network_monitor: NetworkMonitor, cloud_url: str = "http://127.0.0.1:8000"):
        self.storage = storage
        self.network = network_monitor
        self.cloud_url = cloud_url.rstrip("/")
        self.running = False
        self._thread: Optional[threading.Thread] = None

    def post_json_gzip(self, endpoint: str, payload: dict, timeout: float = 5.0) -> Optional[dict]:
        url = f"{self.cloud_url}{endpoint}"
        raw_body = json.dumps(payload).encode("utf-8")
        compressed_body = gzip.compress(raw_body)

        req = urllib.request.Request(
            url,
            data=compressed_body,
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
                "User-Agent": "PipelineSyncWorker/1.0"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status == 200:
                    resp_data = resp.read()
                    # Check if response is gzip
                    if resp.headers.get("Content-Encoding") == "gzip":
                        resp_data = gzip.decompress(resp_data)
                    return json.loads(resp_data.decode("utf-8"))
        except Exception:
            return None
        return None

    def sync_losses_step(self, session_id: str) -> bool:
        losses = self.storage.get_unsynced_losses(session_id, limit=100)
        if not losses:
            return False

        payload = {"session_id": session_id, "losses": losses}
        res = self.post_json_gzip("/api/v1/sync_losses", payload)
        if res and res.get("status") == "ok":
            loss_ids = [l["id"] for l in losses]
            self.storage.mark_losses_synced(loss_ids)
            return True
        return False

    def sync_samples_step(self, session_id: str) -> bool:
        # Section 8: Batches of up to 5,000 samples, oldest first
        samples = self.storage.get_unsynced_samples(session_id, limit=2000)
        if not samples:
            return False

        payload = {"session_id": session_id, "samples": samples}
        res = self.post_json_gzip("/api/v1/sync", payload)
        if res and res.get("status") == "ok":
            keys = [(s["sensor"], s["index"]) for s in samples]
            self.storage.mark_samples_synced(session_id, keys)
            return True
        return False

    def sync_offline_results_step(self, session_id: str) -> bool:
        """Section 14: device uploads offline results so cloud has every window."""
        lines = list(self.storage.get_results_lines(session_id))
        offline_results = [json.loads(line) for line in lines if json.loads(line).get("mode") == "offline"]
        if not offline_results:
            return False

        payload = {"session_id": session_id, "results": offline_results}
        res = self.post_json_gzip("/api/v1/sync_results", payload)
        return bool(res and res.get("status") == "ok")

    def run_loop(self):
        self.running = True
        last_results_sync = 0.0

        while self.running:
            mode = self.network.get_mode()
            if mode != "online":
                time.sleep(0.5)
                continue

            conn = self.storage.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT session_id FROM sessions ORDER BY created_at DESC LIMIT 1;")
            row = cur.fetchone()
            if not row:
                time.sleep(0.5)
                continue
            session_id = row[0]

            did_loss = self.sync_losses_step(session_id)
            did_samples = self.sync_samples_step(session_id)

            now_mono = time.monotonic()
            if now_mono - last_results_sync > 2.0:
                self.sync_offline_results_step(session_id)
                last_results_sync = now_mono

            if not did_loss and not did_samples:
                time.sleep(0.1)
            else:
                # Catching up: tight loop for fast throughput
                pass

    def start(self):
        self._thread = threading.Thread(target=self.run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False
