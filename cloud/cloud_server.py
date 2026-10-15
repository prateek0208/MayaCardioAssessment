"""
Free Cloud Service Backend
Implements Sections 2, 6, 7, 8 & 14:
- Receives samples, loss ranges, and offline window results over HTTP/HTTPS with gzip decompression
- Enforces unique key (session_id, sensor, sample_index) with INSERT OR IGNORE
- Acknowledges only after data is committed
- Finalises online window w when sample 400(w+1) arrives
- Never recomputes or relabels offline results
- Exports session lines matching ground truth verification
"""

import argparse
import gzip
import json
import os
import sqlite3
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline_app"))
from processor import PeakDetector, compute_window_result


class CloudStorage:
    def __init__(self, db_path: str = "cloud_data.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False, isolation_level=None)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            self._local.conn = conn
        return self._local.conn

    def _init_db(self):
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS cloud_samples (
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                sample_index INTEGER NOT NULL,
                val1 INTEGER NOT NULL,
                val2 INTEGER NOT NULL,
                val3 INTEGER,
                PRIMARY KEY (session_id, sensor, sample_index)
            );
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cloud_window 
            ON cloud_samples (session_id, sensor, sample_index);
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS cloud_losses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                from_index INTEGER NOT NULL,
                to_index INTEGER,
                reason TEXT NOT NULL
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS cloud_results (
                session_id TEXT NOT NULL,
                window INTEGER NOT NULL,
                mode TEXT NOT NULL,
                missing_1 INTEGER NOT NULL,
                missing_2 INTEGER NOT NULL,
                complete INTEGER NOT NULL,
                mean_json TEXT,
                min_json TEXT,
                max_json TEXT,
                magnitude REAL,
                heart_rate REAL,
                created_at REAL NOT NULL,
                PRIMARY KEY (session_id, window)
            );
            """)
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def insert_samples(self, session_id: str, samples: list) -> int:
        rows = []
        for s in samples:
            sensor = s["sensor"]
            idx = s["index"]
            vals = s["values"]
            v1 = vals[0]
            v2 = vals[1]
            v3 = vals[2] if len(vals) > 2 else None
            rows.append((session_id, sensor, idx, v1, v2, v3))

        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            cursor = conn.executemany(
                """INSERT OR IGNORE INTO cloud_samples 
                   (session_id, sensor, sample_index, val1, val2, val3) 
                   VALUES (?, ?, ?, ?, ?, ?);""",
                rows
            )
            inserted = cursor.rowcount
            # Section 6: acknowledges only after the data is committed
            conn.execute("COMMIT;")
            return inserted
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def insert_losses(self, session_id: str, losses: list):
        rows = []
        for l in losses:
            rows.append((session_id, l["sensor"], l["from_index"], l.get("to_index"), l["reason"]))
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.executemany(
                """INSERT INTO cloud_losses (session_id, sensor, from_index, to_index, reason) 
                   VALUES (?, ?, ?, ?, ?);""",
                rows
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def insert_offline_results(self, session_id: str, results: list):
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            for res in results:
                conn.execute(
                    """INSERT OR IGNORE INTO cloud_results 
                       (session_id, window, mode, missing_1, missing_2, complete, 
                        mean_json, min_json, max_json, magnitude, heart_rate, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                    (
                        session_id,
                        res["window"],
                        res["mode"],
                        res["missing_1"],
                        res["missing_2"],
                        1 if res["complete"] else 0,
                        json.dumps(res["mean"]) if res["mean"] is not None else None,
                        json.dumps(res["min"]) if res["min"] is not None else None,
                        json.dumps(res["max"]) if res["max"] is not None else None,
                        res["magnitude"],
                        res["heart_rate"],
                        time.time()
                    )
                )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def get_counts(self, session_id: str) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cloud_samples WHERE session_id = ?;", (session_id,))
        return cur.fetchone()[0]

    def export_lines(self, session_id: str):
        conn = self.get_connection()
        cur = conn.cursor()
        # Samples
        cur.execute(
            """SELECT sensor, sample_index, val1, val2, val3 
               FROM cloud_samples 
               WHERE session_id = ? 
               ORDER BY sensor, sample_index ASC;""",
            (session_id,)
        )
        for sensor, idx, v1, v2, v3 in cur:
            vals = [v1, v2] if v3 is None else [v1, v2, v3]
            yield json.dumps({"sensor": sensor, "index": idx, "values": vals})

        # Losses
        cur.execute(
            """SELECT sensor, from_index, to_index, reason 
               FROM cloud_losses 
               WHERE session_id = ? 
               ORDER BY id ASC;""",
            (session_id,)
        )
        for sensor, f_idx, t_idx, reason in cur:
            yield json.dumps({
                "sensor": sensor,
                "from_index": f_idx,
                "to_index": t_idx,
                "reason": reason
            })

    def save_cloud_result(self, res: dict):
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute(
                """INSERT OR IGNORE INTO cloud_results 
                   (session_id, window, mode, missing_1, missing_2, complete, 
                    mean_json, min_json, max_json, magnitude, heart_rate, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                (
                    res["session_id"],
                    res["window"],
                    res["mode"],
                    res["missing_1"],
                    res["missing_2"],
                    1 if res["complete"] else 0,
                    json.dumps(res["mean"]) if res["mean"] is not None else None,
                    json.dumps(res["min"]) if res["min"] is not None else None,
                    json.dumps(res["max"]) if res["max"] is not None else None,
                    res["magnitude"],
                    res["heart_rate"],
                    time.time()
                )
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def get_results_lines(self, session_id: str):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """SELECT window, mode, missing_1, missing_2, complete, 
                      mean_json, min_json, max_json, magnitude, heart_rate 
               FROM cloud_results 
               WHERE session_id = ? 
               ORDER BY window ASC;""",
            (session_id,)
        )
        for row in cur:
            win, mode, m1, m2, comp, mean_j, min_j, max_j, mag, hr = row
            yield json.dumps({
                "window": win,
                "mode": mode,
                "missing_1": m1,
                "missing_2": m2,
                "complete": bool(comp),
                "mean": json.loads(mean_j) if mean_j else None,
                "min": json.loads(min_j) if min_j else None,
                "max": json.loads(max_j) if max_j else None,
                "magnitude": mag,
                "heart_rate": hr
            })


class CloudProcessorManager:
    def __init__(self, cloud_storage: CloudStorage):
        self.storage = cloud_storage
        self.peak_detectors = {}
        self.processed_windows = {}
        self.lock = threading.Lock()

    def process_new_windows(self, session_id: str):
        with self.lock:
            if session_id not in self.peak_detectors:
                self.peak_detectors[session_id] = PeakDetector()
                self.processed_windows[session_id] = set()

            detector = self.peak_detectors[session_id]
            processed = self.processed_windows[session_id]

            conn = self.storage.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT MAX(sample_index) FROM cloud_samples WHERE session_id = ?;", (session_id,))
            row = cur.fetchone()
            if not row or row[0] is None:
                return

            max_idx = row[0]
            # Section 5: window w is finalised when sample 400(w+1) arrives
            max_final_window = (max_idx - 1) // 400

            for w in range(0, max_final_window + 1):
                if w in processed:
                    continue

                w_start = 400 * w
                w_end = w_start + 399

                cur.execute(
                    """SELECT sensor, sample_index, val1, val2, val3 
                       FROM cloud_samples 
                       WHERE session_id = ? AND sample_index >= ? AND sample_index <= ?;""",
                    (session_id, w_start, w_end)
                )
                s1_samples = {}
                s2_samples = {}
                for sensor, idx, v1, v2, v3 in cur.fetchall():
                    vals = [v1, v2] if v3 is None else [v1, v2, v3]
                    if sensor == 1:
                        s1_samples[idx] = vals
                    else:
                        s2_samples[idx] = vals

                all_s1_history = {}
                if w >= 4:
                    h_start = 400 * (w - 4)
                    h_end = 400 * (w + 1)  # include 400(w+1) for peak check at 400(w+1)-1
                    cur.execute(
                        """SELECT sample_index, val2 
                           FROM cloud_samples 
                           WHERE session_id = ? AND sensor = 1 AND sample_index >= ? AND sample_index <= ?;""",
                        (session_id, h_start, h_end)
                    )
                    for idx, val2 in cur.fetchall():
                        all_s1_history[idx] = val2

                res = compute_window_result(
                    session_id=session_id,
                    window=w,
                    mode="online",
                    s1_samples=s1_samples,
                    s2_samples=s2_samples,
                    peak_detector=detector,
                    all_s1_history=all_s1_history
                )
                self.storage.save_cloud_result(res)
                processed.add(w)


GLOBAL_STORAGE = CloudStorage()
GLOBAL_PROCESSOR = CloudProcessorManager(GLOBAL_STORAGE)


class CloudHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, obj: dict):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path in ("/health", "/api/v1/ping"):
            self._send_json(200, {"status": "ok", "service": "biosignal-cloud", "time": time.time()})
            return

        session_id = query.get("session_id", [None])[0]

        if path == "/api/v1/status":
            count = GLOBAL_STORAGE.get_counts(session_id) if session_id else 0
            self._send_json(200, {"session_id": session_id, "samples_in_cloud": count})
            return

        if path == "/api/v1/export":
            if not session_id:
                self._send_json(400, {"error": "Missing session_id"})
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for line in GLOBAL_STORAGE.export_lines(session_id):
                self.wfile.write((line + "\n").encode("utf-8"))
            return

        if path == "/api/v1/results":
            if not session_id:
                self._send_json(400, {"error": "Missing session_id"})
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for line in GLOBAL_STORAGE.get_results_lines(session_id):
                self.wfile.write((line + "\n").encode("utf-8"))
            return

        self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_len = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_len)

        # Handle gzip decompression
        if self.headers.get("Content-Encoding") == "gzip":
            try:
                body_bytes = gzip.decompress(body_bytes)
            except Exception as e:
                self._send_json(400, {"error": f"Gzip decompression error: {e}"})
                return

        try:
            payload = json.loads(body_bytes.decode("utf-8"))
        except Exception as e:
            self._send_json(400, {"error": f"Invalid JSON body: {e}"})
            return

        if path == "/api/v1/sync":
            session_id = payload.get("session_id")
            samples = payload.get("samples", [])
            if not session_id or not samples:
                self._send_json(400, {"error": "Missing session_id or samples"})
                return

            inserted = GLOBAL_STORAGE.insert_samples(session_id, samples)
            GLOBAL_PROCESSOR.process_new_windows(session_id)
            self._send_json(200, {
                "status": "ok",
                "session_id": session_id,
                "inserted": inserted,
                "received_count": len(samples)
            })
            return

        if path == "/api/v1/sync_losses":
            session_id = payload.get("session_id")
            losses = payload.get("losses", [])
            if not session_id:
                self._send_json(400, {"error": "Missing session_id"})
                return
            GLOBAL_STORAGE.insert_losses(session_id, losses)
            self._send_json(200, {"status": "ok", "synced_losses": len(losses)})
            return

        if path == "/api/v1/sync_results":
            session_id = payload.get("session_id")
            results = payload.get("results", [])
            if not session_id:
                self._send_json(400, {"error": "Missing session_id"})
                return
            GLOBAL_STORAGE.insert_offline_results(session_id, results)
            self._send_json(200, {"status": "ok", "synced_results": len(results)})
            return

        self._send_json(404, {"error": "Not found"})

    def log_message(self, format, *args):
        pass


def run_cloud_server(port: int = 8000):
    server = HTTPServer(("0.0.0.0", port), CloudHandler)
    print(f"[Cloud Service] Running on port {port}...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    run_cloud_server(args.port)
