"""
Window Processing and Peak Detection Engine
Implements Section 5 & Section 14 exact rules:
- 1-second window aggregation (400 samples/window)
- Decimal ROUND_HALF_UP on exact integer sums (never float round)
- missing_1, missing_2 = 400 minus samples available in window; complete = true when both 0
- mean, min, max per channel (5 channels total)
- magnitude: mean of sqrt(ch1^2 + ch2^2) over available sensor 2 samples
- heart_rate: continuous peak tracking on sensor 1 channel 2
  - Sample i is peak when: 800 before & 1 after available; larger than both neighbours;
    larger than 0.5 * max(800 prior); at least 100 samples after last accepted peak
    (first peak has no previous peak, condition treated as met).
  - Window w finalised when sample 400(w+1) arrives or is known lost.
  - BPM = 24000 * (K - 1) / (p_K - p_1), rounded to 1 decimal (half up).
  - heart_rate is null if w < 4, if any sensor 1 sample in [400(w-4), 400(w+1)-1] missing, or K < 2.
- Config flag: HR_REQUIRE_BOTH_SENSORS (default False).
"""

from decimal import Decimal, ROUND_HALF_UP
import math
from typing import Dict, List, Optional, Tuple

HR_REQUIRE_BOTH_SENSORS = False


def round_half_up(val: Optional[Decimal], places: int = 1) -> Optional[float]:
    """Rounds Decimal half-up to specified places, returning float."""
    if val is None:
        return None
    q = Decimal("10") ** -places
    return float(val.quantize(q, rounding=ROUND_HALF_UP))


class PeakDetector:
    """
    Maintains continuous peak detection over the stream.
    Peaks are found continuously over the whole stream, not per window.
    """
    def __init__(self):
        self.last_peak_index: Optional[int] = None
        self.detected_peaks: List[int] = []

    def find_peaks_up_to(self, s1_ch2_dict: Dict[int, int], max_index_to_check: int):
        """
        Scans from index 800 up to max_index_to_check.
        Evaluates peak conditions against last accepted peak.
        """
        start = 800
        if self.detected_peaks:
            # Continue checking after the last checked position
            start = max(800, self.detected_peaks[-1] + 1)

        for i in range(start, max_index_to_check + 1):
            if i in self.detected_peaks:
                continue

            # 1. Sample i+1 must be present
            if (i + 1) not in s1_ch2_dict:
                continue

            # 2. Check 800 prior samples exist
            prior_vals = []
            has_all_800 = True
            for k in range(i - 800, i):
                if k not in s1_ch2_dict:
                    has_all_800 = False
                    break
                prior_vals.append(s1_ch2_dict[k])

            if not has_all_800 or len(prior_vals) != 800:
                continue

            xi = s1_ch2_dict[i]
            x_prev = s1_ch2_dict[i - 1]
            x_next = s1_ch2_dict[i + 1]

            # Larger than both neighbours
            if not (xi > x_prev and xi > x_next):
                continue

            # Larger than half the max of 800 prior samples
            max_prior = max(prior_vals)
            # Use Decimal or integer arithmetic
            if not (xi * 2 > max_prior):  # xi > 0.5 * max_prior
                continue

            # At least 100 samples after previous peak (first peak has no previous peak)
            if self.last_peak_index is not None:
                if (i - self.last_peak_index) < 100:
                    continue

            # Accepted peak!
            self.last_peak_index = i
            self.detected_peaks.append(i)


def compute_window_result(
    session_id: str,
    window: int,
    mode: str,
    s1_samples: Dict[int, List[int]],
    s2_samples: Dict[int, List[int]],
    peak_detector: PeakDetector,
    all_s1_history: Dict[int, int],
    hr_require_both: bool = HR_REQUIRE_BOTH_SENSORS
) -> dict:
    start_idx = 400 * window
    end_idx = start_idx + 399

    avail_s1 = [i for i in range(start_idx, end_idx + 1) if i in s1_samples]
    avail_s2 = [i for i in range(start_idx, end_idx + 1) if i in s2_samples]

    missing_1 = 400 - len(avail_s1)
    missing_2 = 400 - len(avail_s2)
    complete = (missing_1 == 0 and missing_2 == 0)

    # Channel stats (5 channels total)
    # Sensor 1 (3 channels)
    if avail_s1:
        n1 = Decimal(len(avail_s1))
        sum_c1 = Decimal(sum(s1_samples[i][0] for i in avail_s1))
        sum_c2 = Decimal(sum(s1_samples[i][1] for i in avail_s1))
        sum_c3 = Decimal(sum(s1_samples[i][2] for i in avail_s1))

        mean_s1 = [
            round_half_up(sum_c1 / n1, 1),
            round_half_up(sum_c2 / n1, 1),
            round_half_up(sum_c3 / n1, 1),
        ]
        min_s1 = [
            min(s1_samples[i][0] for i in avail_s1),
            min(s1_samples[i][1] for i in avail_s1),
            min(s1_samples[i][2] for i in avail_s1),
        ]
        max_s1 = [
            max(s1_samples[i][0] for i in avail_s1),
            max(s1_samples[i][1] for i in avail_s1),
            max(s1_samples[i][2] for i in avail_s1),
        ]
    else:
        mean_s1, min_s1, max_s1 = None, None, None

    # Sensor 2 (2 channels)
    if avail_s2:
        n2 = Decimal(len(avail_s2))
        sum_s2_c1 = Decimal(sum(s2_samples[i][0] for i in avail_s2))
        sum_s2_c2 = Decimal(sum(s2_samples[i][1] for i in avail_s2))

        mean_s2 = [
            round_half_up(sum_s2_c1 / n2, 1),
            round_half_up(sum_s2_c2 / n2, 1),
        ]
        min_s2 = [
            min(s2_samples[i][0] for i in avail_s2),
            min(s2_samples[i][1] for i in avail_s2),
        ]
        max_s2 = [
            max(s2_samples[i][0] for i in avail_s2),
            max(s2_samples[i][1] for i in avail_s2),
        ]

        # Magnitude = mean of sqrt(ch1^2 + ch2^2)
        mag_sum = sum(math.sqrt(s2_samples[i][0] ** 2 + s2_samples[i][1] ** 2) for i in avail_s2)
        magnitude = round_half_up(Decimal(str(mag_sum)) / n2, 1)
    else:
        mean_s2, min_s2, max_s2 = None, None, None
        magnitude = None

    mean_res = {"sensor_1": mean_s1, "sensor_2": mean_s2}
    min_res = {"sensor_1": min_s1, "sensor_2": min_s2}
    max_res = {"sensor_1": max_s1, "sensor_2": max_s2}

    # Heart Rate
    heart_rate = None
    if window >= 4:
        five_sec_start = 400 * (window - 4)
        five_sec_end = 400 * (window + 1) - 1  # 2000 samples

        # Check for missing samples in sensor 1
        s1_missing = any(i not in all_s1_history for i in range(five_sec_start, five_sec_end + 1))
        
        # Check sensor 2 if flag enabled
        s2_missing = False
        if hr_require_both:
            s2_missing = any(i not in s2_samples for i in range(five_sec_start, five_sec_end + 1))

        if not s1_missing and not s2_missing:
            # Scan peaks up to five_sec_end
            peak_detector.find_peaks_up_to(all_s1_history, five_sec_end)

            peaks_in_window = [p for p in peak_detector.detected_peaks if five_sec_start <= p <= five_sec_end]
            K = len(peaks_in_window)

            if K >= 2:
                p_1 = peaks_in_window[0]
                p_K = peaks_in_window[-1]
                gap_samples = p_K - p_1
                if gap_samples > 0:
                    # Exact formula from Section 5: heart_rate = 24000 * (K - 1) / (p_K - p_1)
                    hr_dec = (Decimal(24000) * Decimal(K - 1)) / Decimal(gap_samples)
                    heart_rate = round_half_up(hr_dec, 1)

    return {
        "session_id": session_id,
        "window": window,
        "mode": mode,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "mean": mean_res,
        "min": min_res,
        "max": max_res,
        "magnitude": magnitude,
        "heart_rate": heart_rate,
    }
