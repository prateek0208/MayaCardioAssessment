"""
Storage Engine for Never Lose a Heartbeat Pipeline
Implements Section 3, 4 & 14 exact rules:
- SQLite WAL mode with synchronous = FULL (survives sudden power cuts without rollbacks)
- PRAGMA integrity_check at startup
- Disk space monitoring: reserve threshold for loss records, prevents disk overflow corruption
- Gap detection for device_down, mock_no_data, read_timeout, restart_gap
"""

import json
import os
import shutil
import sqlite3
import threading
import time
from typing import Dict, List, Optional, Tuple

MIN_DISK_FREE_BYTES = 10 * 1024 * 1024  # 10 MB safety reserve


class StorageEngine:
    def __init__(self, db_path: str = "pipeline.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False,
                isolation_level=None
            )
            # Section 4: journal_mode=WAL and synchronous=FULL
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = FULL;")
            conn.execute("PRAGMA temp_store = MEMORY;")
            self._local.conn = conn
        return self._local.conn

    def _init_db(self):
        conn = self.get_connection()
        # Section 4: Run PRAGMA integrity_check at startup
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check;")
        res = cur.fetchone()
        if not res or res[0] != "ok":
            raise RuntimeError(f"Database integrity check failed: {res}")

        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                created_at REAL NOT NULL,
                ended_at REAL
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                sample_index INTEGER NOT NULL,
                val1 INTEGER NOT NULL,
                val2 INTEGER NOT NULL,
                val3 INTEGER,
                synced INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (session_id, sensor, sample_index)
            );
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_samples_sync 
            ON samples (synced, session_id, sensor, sample_index);
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_samples_window 
            ON samples (session_id, sensor, sample_index);
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS lost_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                from_index INTEGER NOT NULL,
                to_index INTEGER,
                reason TEXT NOT NULL,
                synced INTEGER NOT NULL DEFAULT 0
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
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
                PRIMARY KEY (session_id, window, mode)
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """)

            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def check_disk_space(self) -> bool:
        """Section 3: check free space, keep reserve for loss records."""
        try:
            folder = os.path.dirname(os.path.abspath(self.db_path)) or "."
            free_bytes = shutil.disk_usage(folder).free
            return free_bytes > MIN_DISK_FREE_BYTES
        except Exception:
            return True

    def register_session(self, session_id: str, timestamp: float):
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute(
                "INSERT OR IGNORE INTO sessions (session_id, created_at) VALUES (?, ?);",
                (session_id, timestamp)
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def insert_samples(self, session_id: str, sensor: int, first_index: int, data: List[List[int]]) -> int:
        if not data:
            return 0

        # Section 3: Disk full check
        if not self.check_disk_space():
            count = len(data)
            self.record_loss(session_id, sensor, first_index, first_index + count - 1, reason="disk_full")
            return 0

        rows = []
        for i, sample in enumerate(data):
            idx = first_index + i
            v1 = sample[0]
            v2 = sample[1]
            v3 = sample[2] if len(sample) > 2 else None
            rows.append((session_id, sensor, idx, v1, v2, v3))

        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            cursor = conn.executemany(
                """INSERT OR IGNORE INTO samples 
                   (session_id, sensor, sample_index, val1, val2, val3, synced) 
                   VALUES (?, ?, ?, ?, ?, ?, 0);""",
                rows
            )
            inserted = cursor.rowcount
            conn.execute("COMMIT;")
            return inserted
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def record_loss(self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str):
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute(
                """INSERT INTO lost_ranges 
                   (session_id, sensor, from_index, to_index, reason, synced) 
                   VALUES (?, ?, ?, ?, ?, 0);""",
                (session_id, sensor, from_index, to_index, reason)
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def get_highest_contiguous_index(self, session_id: str, sensor: int) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(sample_index) FROM samples WHERE session_id = ? AND sensor = ?;",
            (session_id, sensor)
        )
        row = cur.fetchone()
        return row[0] if (row and row[0] is not None) else -1

    def get_window_samples(self, session_id: str, start_index: int, count: int = 400) -> Dict[int, Dict[int, List[int]]]:
        conn = self.get_connection()
        end_index = start_index + count - 1
        cur = conn.cursor()
        cur.execute(
            """SELECT sensor, sample_index, val1, val2, val3 
               FROM samples 
               WHERE session_id = ? AND sample_index >= ? AND sample_index <= ?
               ORDER BY sample_index ASC;""",
            (session_id, start_index, end_index)
        )
        out = {1: {}, 2: {}}
        for sensor, idx, v1, v2, v3 in cur.fetchall():
            vals = [v1, v2] if v3 is None else [v1, v2, v3]
            out[sensor][idx] = vals
        return out

    def get_samples_range(self, session_id: str, sensor: int, start_idx: int, end_idx: int) -> Dict[int, List[int]]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """SELECT sample_index, val1, val2, val3 
               FROM samples 
               WHERE session_id = ? AND sensor = ? AND sample_index >= ? AND sample_index <= ?
               ORDER BY sample_index ASC;""",
            (session_id, sensor, start_idx, end_idx)
        )
        res = {}
        for idx, v1, v2, v3 in cur.fetchall():
            res[idx] = [v1, v2] if v3 is None else [v1, v2, v3]
        return res

    def save_result(self, session_id: str, window: int, mode: str, missing_1: int, missing_2: int,
                    complete: bool, mean_dict: Optional[dict], min_dict: Optional[dict],
                    max_dict: Optional[dict], magnitude: Optional[float],
                    heart_rate: Optional[float], timestamp: float):
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute(
                """INSERT OR REPLACE INTO results 
                   (session_id, window, mode, missing_1, missing_2, complete, 
                    mean_json, min_json, max_json, magnitude, heart_rate, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                (
                    session_id,
                    window,
                    mode,
                    missing_1,
                    missing_2,
                    1 if complete else 0,
                    json.dumps(mean_dict) if mean_dict is not None else None,
                    json.dumps(min_dict) if min_dict is not None else None,
                    json.dumps(max_dict) if max_dict is not None else None,
                    magnitude,
                    heart_rate,
                    timestamp
                )
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def get_unsynced_samples(self, session_id: str, limit: int = 5000) -> List[dict]:
        """Section 8: batches of up to 5,000 samples, oldest first."""
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """SELECT sensor, sample_index, val1, val2, val3 
               FROM samples 
               WHERE session_id = ? AND synced = 0 
               ORDER BY sensor, sample_index ASC 
               LIMIT ?;""",
            (session_id, limit)
        )
        batch = []
        for sensor, idx, v1, v2, v3 in cur.fetchall():
            vals = [v1, v2] if v3 is None else [v1, v2, v3]
            batch.append({
                "sensor": sensor,
                "index": idx,
                "values": vals
            })
        return batch

    def mark_samples_synced(self, session_id: str, sample_keys: List[Tuple[int, int]]):
        if not sample_keys:
            return
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.executemany(
                """UPDATE samples SET synced = 1 
                   WHERE session_id = ? AND sensor = ? AND sample_index = ?;""",
                [(session_id, sensor, idx) for sensor, idx in sample_keys]
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def get_unsynced_losses(self, session_id: str, limit: int = 100) -> List[dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """SELECT id, sensor, from_index, to_index, reason 
               FROM lost_ranges 
               WHERE session_id = ? AND synced = 0 
               ORDER BY id ASC LIMIT ?;""",
            (session_id, limit)
        )
        losses = []
        for lid, sensor, f_idx, t_idx, reason in cur.fetchall():
            losses.append({
                "id": lid,
                "sensor": sensor,
                "from_index": f_idx,
                "to_index": t_idx,
                "reason": reason
            })
        return losses

    def mark_losses_synced(self, loss_ids: List[int]):
        if not loss_ids:
            return
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.executemany(
                "UPDATE lost_ranges SET synced = 1 WHERE id = ?;",
                [(lid,) for lid in loss_ids]
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def get_counts(self, session_id: str) -> Tuple[int, int, int]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM samples WHERE session_id = ?;", (session_id,))
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM samples WHERE session_id = ? AND synced = 1;", (session_id,))
        synced = cur.fetchone()[0]

        backlog = total - synced
        return total, synced, backlog

    def export_session_lines(self, session_id: str):
        conn = self.get_connection()
        cur = conn.cursor()

        # Section 9: samples first
        cur.execute(
            """SELECT sensor, sample_index, val1, val2, val3 
               FROM samples 
               WHERE session_id = ? 
               ORDER BY sensor, sample_index ASC;""",
            (session_id,)
        )
        for sensor, idx, v1, v2, v3 in cur:
            vals = [v1, v2] if v3 is None else [v1, v2, v3]
            yield json.dumps({"sensor": sensor, "index": idx, "values": vals})

        # Then losses
        cur.execute(
            """SELECT sensor, from_index, to_index, reason 
               FROM lost_ranges 
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

    def get_results_lines(self, session_id: str):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """SELECT window, mode, missing_1, missing_2, complete, 
                      mean_json, min_json, max_json, magnitude, heart_rate 
               FROM results 
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
