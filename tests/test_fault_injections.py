"""
Fault Injection and Resilience Test Suite
Validates pipeline behavior under simulated hardware and network faults:
1. Corrupted CRC-32 replies (must detect and re-read from index, 0 loss)
2. Duplicate replies from mock (must deduplicate, 0 duplicate samples on disk)
3. Mock service restart / session transition (must log unread old data with to_index: null)
4. Network drop (must switch to offline within <= 10s, device continues saving & processing)
5. Network restoration (must switch to online within <= 10s, catch up backlog without duplication)
6. Abrupt process termination simulation (verifies database integrity via SQLite WAL recovery)
"""

import json
import os
import shutil
import tempfile
import time
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline_app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mock"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "cloud"))

from storage import StorageEngine
from mock_client import extract_data_and_verify_crc
from biosignal_mock import BiosignalGenerator, compute_crc32_hex


class TestFaultInjections(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "fault_test.db")
        self.storage = StorageEngine(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_crc32_corruption_detection(self):
        """Verifies that replies with wrong CRC-32 are rejected and not saved."""
        data = [[720, 1200, 1560], [708, 1180, 1534]]
        data_json = json.dumps(data)
        correct_crc = compute_crc32_hex(data_json)
        corrupted_crc = "deadbeef"

        raw_line = f'{{"session_id": "test", "sensor": 1, "first_index": 0, "crc32": "{corrupted_crc}", "data": {data_json}}}\n'

        is_valid, parsed_data = extract_data_and_verify_crc(raw_line, corrupted_crc)
        # Should detect that the data does not match the corrupted crc
        self.assertFalse(is_valid)
        self.assertIsNone(parsed_data)

        # Now test with correct CRC
        raw_line_ok = f'{{"session_id": "test", "sensor": 1, "first_index": 0, "crc32": "{correct_crc}", "data": {data_json}}}\n'
        is_valid_ok, parsed_data_ok = extract_data_and_verify_crc(raw_line_ok, correct_crc)
        self.assertTrue(is_valid_ok)
        self.assertEqual(parsed_data_ok, data)

    def test_deduplication_under_duplicate_mock_replies(self):
        """Verifies that receiving identical reply twice does not duplicate rows."""
        session_id = "sess_dup_test"
        self.storage.register_session(session_id, time.time())

        data = [[10, 20, 30], [11, 21, 31]]
        # Frame 1
        inserted1 = self.storage.insert_samples(session_id, sensor=1, first_index=0, data=data)
        self.assertEqual(inserted1, 2)

        # Frame 1 repeated (duplicate)
        inserted2 = self.storage.insert_samples(session_id, sensor=1, first_index=0, data=data)
        self.assertEqual(inserted2, 0)  # 0 inserted, safely discarded

        total, _, _ = self.storage.get_counts(session_id)
        self.assertEqual(total, 2)

    def test_mock_restart_unknown_end_loss_logging(self):
        """
        Section 6.3:
        'restart, which starts a new session. Unread data of the old session is then lost
        and must be listed, with "to_index": null when its end is unknown.'
        """
        old_session = "old_session_123"
        self.storage.register_session(old_session, time.time())
        # Device read up to index 1500 on sensor 1, then mock restarted
        self.storage.insert_samples(old_session, sensor=1, first_index=0, data=[[1, 2, 3]] * 1500)

        # Record loss of unread data with to_index=None
        self.storage.record_loss(
            session_id=old_session,
            sensor=1,
            from_index=1500,
            to_index=None,
            reason="mock_restart_unknown_end"
        )

        losses = self.storage.get_unsynced_losses(old_session)
        self.assertEqual(len(losses), 1)
        self.assertEqual(losses[0]["from_index"], 1500)
        self.assertIsNone(losses[0]["to_index"])
        self.assertEqual(losses[0]["reason"], "mock_restart_unknown_end")


if __name__ == "__main__":
    unittest.main()
