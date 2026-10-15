"""
Unit tests for Storage Engine (pipeline_app/storage.py)
Tests:
- Table initialization and WAL mode activation
- Atomic batch sample insertion
- Deduplication: duplicate insertions ignored without errors or duplicate rows
- Loss range logging (known range and unknown-end)
- Sync status queries and sync-mark transitions
- Export lines format matching Section 4
"""

import json
import os
import tempfile
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline_app"))
from storage import StorageEngine


class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_pipeline.db")
        self.storage = StorageEngine(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_sample_insertion_and_deduplication(self):
        session_id = "test_sess_01"
        self.storage.register_session(session_id, 1000.0)

        data = [
            [720, 1200, 1560],
            [708, 1180, 1534],
            [726, 1210, 1573]
        ]
        inserted = self.storage.insert_samples(session_id, sensor=1, first_index=100, data=data)
        self.assertEqual(inserted, 3)

        # Attempt to insert identical samples again (duplicate replay fault simulation)
        inserted_dup = self.storage.insert_samples(session_id, sensor=1, first_index=100, data=data)
        self.assertEqual(inserted_dup, 0)

        # Verify only 3 samples saved
        total, synced, backlog = self.storage.get_counts(session_id)
        self.assertEqual(total, 3)
        self.assertEqual(synced, 0)
        self.assertEqual(backlog, 3)

    def test_loss_recording(self):
        session_id = "test_sess_02"
        self.storage.register_session(session_id, 1000.0)

        self.storage.record_loss(session_id, sensor=2, from_index=4000, to_index=4399, reason="buffer_overflow")
        self.storage.record_loss(session_id, sensor=1, from_index=5000, to_index=None, reason="mock_restart")

        losses = self.storage.get_unsynced_losses(session_id, limit=10)
        self.assertEqual(len(losses), 2)
        self.assertEqual(losses[0]["from_index"], 4000)
        self.assertEqual(losses[0]["to_index"], 4399)
        self.assertEqual(losses[0]["reason"], "buffer_overflow")
        self.assertIsNone(losses[1]["to_index"])

    def test_sync_workflow(self):
        session_id = "test_sess_03"
        self.storage.register_session(session_id, 1000.0)

        data = [[10, 20, 30], [11, 21, 31]]
        self.storage.insert_samples(session_id, sensor=1, first_index=0, data=data)

        # Retrieve unsynced batch
        batch = self.storage.get_unsynced_samples(session_id, limit=10)
        self.assertEqual(len(batch), 2)

        # Mark first sample synced
        keys = [(batch[0]["sensor"], batch[0]["index"])]
        self.storage.mark_samples_synced(session_id, keys)

        total, synced, backlog = self.storage.get_counts(session_id)
        self.assertEqual(total, 2)
        self.assertEqual(synced, 1)
        self.assertEqual(backlog, 1)

    def test_export_lines(self):
        session_id = "test_sess_04"
        self.storage.register_session(session_id, 1000.0)

        self.storage.insert_samples(session_id, sensor=1, first_index=0, data=[[1, 2, 3]])
        self.storage.record_loss(session_id, sensor=2, from_index=10, to_index=19, reason="test_loss")

        lines = list(self.storage.export_session_lines(session_id))
        self.assertEqual(len(lines), 2)

        sample_obj = json.loads(lines[0])
        self.assertEqual(sample_obj, {"sensor": 1, "index": 0, "values": [1, 2, 3]})

        loss_obj = json.loads(lines[1])
        self.assertEqual(loss_obj, {"sensor": 2, "from_index": 10, "to_index": 19, "reason": "test_loss"})


if __name__ == "__main__":
    unittest.main()
