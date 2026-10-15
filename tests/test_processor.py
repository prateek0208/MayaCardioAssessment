"""
Unit tests for Section 3 Processing Logic & Mathematics
Tests:
- Missing counts and complete boolean flag
- Channel statistics (mean, min, max) rounded to 1 decimal place
- Sensor 2 magnitude calculation
- Heart rate peak detection with all 4 constraints
- 5-second moving window heart rate formula
- Heart rate null invalidation rules (w < 4, missing samples, < 2 peaks)
"""

import math
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline_app"))
from processor import PeakDetector, compute_window_result


class TestProcessingLogic(unittest.TestCase):
    def test_window_stats_and_magnitude(self):
        # Window 0: samples 0 to 399
        s1 = {}
        s2 = {}
        # Fill sensor 1: 3 channels
        # ch1 = 10, ch2 = 20, ch3 = 30 for all samples
        for i in range(400):
            s1[i] = [10, 20, 30]
            s2[i] = [3, 4]  # sqrt(3^2 + 4^2) = 5.0

        detector = PeakDetector()
        res = compute_window_result(
            session_id="test-session",
            window=0,
            mode="offline",
            s1_samples=s1,
            s2_samples=s2,
            peak_detector=detector,
            all_s1_history={}
        )

        self.assertEqual(res["window"], 0)
        self.assertEqual(res["mode"], "offline")
        self.assertEqual(res["missing_1"], 0)
        self.assertEqual(res["missing_2"], 0)
        self.assertTrue(res["complete"])

        # Check mean, min, max
        self.assertEqual(res["mean"]["sensor_1"], [10.0, 20.0, 30.0])
        self.assertEqual(res["min"]["sensor_1"], [10, 20, 30])
        self.assertEqual(res["max"]["sensor_1"], [10, 20, 30])

        self.assertEqual(res["mean"]["sensor_2"], [3.0, 4.0])
        self.assertEqual(res["min"]["sensor_2"], [3, 4])
        self.assertEqual(res["max"]["sensor_2"], [3, 4])

        # Magnitude = 5.0
        self.assertEqual(res["magnitude"], 5.0)

        # Heart rate null for w < 4
        self.assertIsNone(res["heart_rate"])

    def test_missing_samples(self):
        s1 = {0: [1, 2, 3]}  # only 1 sample present
        s2 = {}              # 0 samples present

        detector = PeakDetector()
        res = compute_window_result(
            session_id="test-session",
            window=0,
            mode="offline",
            s1_samples=s1,
            s2_samples=s2,
            peak_detector=detector,
            all_s1_history={}
        )

        self.assertEqual(res["missing_1"], 399)
        self.assertEqual(res["missing_2"], 400)
        self.assertFalse(res["complete"])
        self.assertIsNone(res["magnitude"])
        self.assertIsNone(res["mean"]["sensor_2"])

    def test_heart_rate_detection(self):
        """
        Tests ECG peak detection and BPM calculation for window w >= 4:
        At 60 bpm, peaks are separated by 400 samples (1.0s).
        Window 4 covers samples 1600 to 1999.
        5-second interval: [400*(4-4), 400*(4+1)-1] = [0, 1999].
        """
        all_s1 = {}
        # Construct baseline around 1000 with peaks every 400 samples at indices: 800, 1200, 1600
        for i in range(2001):
            all_s1[i] = 1000

        # Create peaks at 800, 1200, 1600
        # For peak at 800:
        # Needs 800 samples before (0-799) where max is 1000.
        # Condition 3: xi > 0.5 * 1000 = 500.
        # Condition 2: larger than neighbours.
        for p in [800, 1200, 1600]:
            all_s1[p - 1] = 1200
            all_s1[p] = 2500
            all_s1[p + 1] = 1200

        s1_w4 = {i: [600, all_s1[i], 1300] for i in range(1600, 2000)}
        s2_w4 = {i: [400000, 150000] for i in range(1600, 2000)}

        detector = PeakDetector()
        res = compute_window_result(
            session_id="test-session",
            window=4,
            mode="online",
            s1_samples=s1_w4,
            s2_samples=s2_w4,
            peak_detector=detector,
            all_s1_history=all_s1
        )

        self.assertEqual(res["window"], 4)
        self.assertEqual(res["mode"], "online")
        self.assertTrue(res["complete"])

        # Peaks detected at 800, 1200, 1600
        # Gaps: (1200-800)=400, (1600-1200)=400 => mean gap = 400 samples = 1.0 sec
        # Heart rate = 60 / 1.0 = 60.0 bpm
        self.assertEqual(res["heart_rate"], 60.0)

    def test_heart_rate_null_on_missing_sample(self):
        """Rule 3: heart_rate is null if any sample in those 5 seconds is missing."""
        all_s1 = {i: 1000 for i in range(2001)}
        for p in [800, 1200, 1600]:
            all_s1[p - 1] = 1200
            all_s1[p] = 2500
            all_s1[p + 1] = 1200

        # Remove single sample at index 500
        del all_s1[500]

        s1_w4 = {i: [600, all_s1.get(i, 1000), 1300] for i in range(1600, 2000)}
        s2_w4 = {i: [400000, 150000] for i in range(1600, 2000)}

        detector = PeakDetector()
        res = compute_window_result(
            session_id="test-session",
            window=4,
            mode="online",
            s1_samples=s1_w4,
            s2_samples=s2_w4,
            peak_detector=detector,
            all_s1_history=all_s1
        )

        self.assertIsNone(res["heart_rate"])


if __name__ == "__main__":
    unittest.main()
