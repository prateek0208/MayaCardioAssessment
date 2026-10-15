"""
Ground Truth Verifier tool (check_ground_truth)
Compares exported session data (JSON Lines) against the ground truth generator.
Verifies Criteria 1 & 2:
- Criterion 1: Every sample received is unchanged, exactly once.
- Criterion 2: Every sample not present is listed as lost with a reason (no silent loss).
- Deductions check: flags any silent gaps or wrong values (-5 pts each).
"""

import argparse
import json
import sys
from biosignal_mock import BiosignalGenerator


def check_export(export_lines, mode="signal", seed=7, duration=3600):
    generator = BiosignalGenerator(mode=mode, seed=seed, duration=duration)

    samples = {1: {}, 2: {}}
    losses = {1: [], 2: []}

    parse_errors = 0
    duplicate_count = 0
    corrupt_value_count = 0

    for line_num, line in enumerate(export_lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception as e:
            print(f"[ERROR] Line {line_num}: Failed to parse JSON: {e}")
            parse_errors += 1
            continue

        if "values" in item:
            # Sample record
            sensor = item.get("sensor")
            idx = item.get("index")
            vals = item.get("values")
            if sensor not in (1, 2) or idx is None or not isinstance(vals, list):
                print(f"[ERROR] Line {line_num}: Malformed sample record: {line}")
                parse_errors += 1
                continue

            # Check duplicate
            if idx in samples[sensor]:
                print(f"[ERROR] Duplicate sample detected: sensor {sensor}, index {idx}")
                duplicate_count += 1
            else:
                samples[sensor][idx] = vals

            # Check values match ground truth
            expected_vals = generator.get_sample(sensor, idx)
            if expected_vals is None:
                print(f"[ERROR] Index {idx} out of generator range [0, {generator.total_samples})")
                corrupt_value_count += 1
            elif vals != expected_vals:
                print(f"[ERROR] Value mismatch sensor {sensor}, index {idx}: got {vals}, expected {expected_vals}")
                corrupt_value_count += 1

        elif "from_index" in item:
            # Loss record
            sensor = item.get("sensor")
            from_idx = item.get("from_index")
            to_idx = item.get("to_index")
            reason = item.get("reason")
            if sensor not in (1, 2) or from_idx is None or not reason:
                print(f"[ERROR] Line {line_num}: Malformed loss record: {line}")
                parse_errors += 1
                continue
            losses[sensor].append((from_idx, to_idx, reason))

    print("\n--- Summary Verification Results ---")
    print(f"Sensor 1 samples received: {len(samples[1])}")
    print(f"Sensor 2 samples received: {len(samples[2])}")
    print(f"Sensor 1 loss entries: {len(losses[1])}")
    print(f"Sensor 2 loss entries: {len(losses[2])}")
    print(f"Parse errors: {parse_errors}")
    print(f"Duplicates: {duplicate_count}")
    print(f"Corrupt values: {corrupt_value_count}")

    # Check for silent loss (gaps not accounted for in loss records)
    silent_losses = 0
    for sensor in (1, 2):
        if not samples[sensor]:
            continue
        min_idx = min(samples[sensor].keys())
        max_idx = max(samples[sensor].keys())

        # Build set of all declared lost indices
        declared_lost = set()
        for from_i, to_i, r in losses[sensor]:
            if to_i is not None:
                for lost_idx in range(from_i, to_i + 1):
                    declared_lost.add(lost_idx)

        # Check every index in span
        for i in range(min_idx, max_idx + 1):
            if i not in samples[sensor] and i not in declared_lost:
                print(f"[SILENT LOSS] Sensor {sensor}, index {i} is missing and not listed in lost records!")
                silent_losses += 1
                if silent_losses >= 20:
                    print("[SILENT LOSS] Too many silent losses (> 20), truncating output...")
                    break

    passed = (parse_errors == 0 and duplicate_count == 0 and corrupt_value_count == 0 and silent_losses == 0)
    if passed:
        print("\n>>> GROUND TRUTH CHECK: PASSED (100% data integrity, 0 silent losses) <<<")
    else:
        print("\n>>> GROUND TRUTH CHECK: FAILED <<<")
    return passed


def main():
    parser = argparse.ArgumentParser(description="Check pipeline export against ground truth")
    parser.add_argument("--export-file", type=str, default=None, help="Path to export file (reads stdin if omitted)")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duration", type=int, default=3600)
    args = parser.parse_args()

    if args.export_file:
        with open(args.export_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    success = check_export(lines, mode=args.mode, seed=args.seed, duration=args.duration)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
