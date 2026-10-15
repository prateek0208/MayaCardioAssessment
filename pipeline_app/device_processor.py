"""
Device Window Processor
Implements Section 5, 7 & 14 offline window computation:
- Finalises window w when sample 400(w+1) arrives or is known lost
- Saves results with mode=offline
- Continuous peak tracking matching Section 5
"""

import threading
import time
from typing import Dict, Set
from network import NetworkMonitor
from processor import PeakDetector, compute_window_result
from storage import StorageEngine


class DeviceProcessor:
    def __init__(self, storage: StorageEngine, network_monitor: NetworkMonitor):
        self.storage = storage
        self.network = network_monitor
        self.running = False
        self._thread = None
        self.peak_detectors: Dict[str, PeakDetector] = {}
        self.processed_windows: Dict[str, Set[int]] = {}

    def run_loop(self):
        self.running = True
        while self.running:
            mode = self.network.get_mode()
            conn = self.storage.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT session_id FROM sessions ORDER BY created_at DESC LIMIT 1;")
            row = cur.fetchone()
            if not row:
                time.sleep(0.5)
                continue
            session_id = row[0]

            if session_id not in self.peak_detectors:
                self.peak_detectors[session_id] = PeakDetector()
                self.processed_windows[session_id] = set()

            detector = self.peak_detectors[session_id]
            processed = self.processed_windows[session_id]

            cur.execute(
                "SELECT MAX(sample_index) FROM samples WHERE session_id = ?;",
                (session_id,)
            )
            max_row = cur.fetchone()
            if not max_row or max_row[0] is None:
                time.sleep(0.2)
                continue

            max_idx = max_row[0]
            # Section 5: window w is finalised when sample 400(w+1) arrives
            max_final_window = (max_idx - 1) // 400

            # If offline, generate offline results
            if mode == "offline":
                for w in range(0, max_final_window + 1):
                    if w in processed:
                        continue

                    win_samples = self.storage.get_window_samples(session_id, 400 * w, 400)
                    s1_samples = win_samples.get(1, {})
                    s2_samples = win_samples.get(2, {})

                    all_s1_history = {}
                    if w >= 4:
                        h_start = 400 * (w - 4)
                        h_end = 400 * (w + 1)
                        s1_range = self.storage.get_samples_range(session_id, 1, h_start, h_end)
                        for idx, vals in s1_range.items():
                            all_s1_history[idx] = vals[1]

                    res = compute_window_result(
                        session_id=session_id,
                        window=w,
                        mode="offline",
                        s1_samples=s1_samples,
                        s2_samples=s2_samples,
                        peak_detector=detector,
                        all_s1_history=all_s1_history
                    )

                    self.storage.save_result(
                        session_id=session_id,
                        window=w,
                        mode="offline",
                        missing_1=res["missing_1"],
                        missing_2=res["missing_2"],
                        complete=res["complete"],
                        mean_dict=res["mean"],
                        min_dict=res["min"],
                        max_dict=res["max"],
                        magnitude=res["magnitude"],
                        heart_rate=res["heart_rate"],
                        timestamp=time.time()
                    )
                    processed.add(w)

            time.sleep(0.2)

    def start(self):
        self._thread = threading.Thread(target=self.run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False
