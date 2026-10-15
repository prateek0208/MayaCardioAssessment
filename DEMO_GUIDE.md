# 5-Minute Demonstration Video Guide & Script
**MayaCardio Software Systems Engineer Assessment — October 2026**  
**Required Deliverable: Video Demonstration (< 5 minutes)**

---

## Overview

The demonstration video must clearly show three physical failure events and the pipeline's automated recovery with zero silent data loss:
1. **Network Cut and Restoration** (shows sub-10s mode switch and catch-up).
2. **Process Kill (`kill -9`)** (shows systemd auto-restart and `device_down` logging).
3. **Sudden Power Cut** (shows WAL recovery, bounded loss $\le 1\text{s}$, and ground-truth verification).

---

## Video Timeline & Script (Total: 4 min 30 sec)

### [0:00 - 0:45] Introduction & Baseline Setup
- **Screen View**: 2 terminal windows side-by-side.
  - Left terminal: Raspberry Pi 4 board.
  - Right terminal: Cloud Ingestion Server.
- **Actions**:
  1. Show system info:
     ```bash
     uname -m               # Outputs: aarch64
     lscpu | grep "Model"   # Cortex-A72 (Raspberry Pi 4)
     free -h                # Shows 2.0 GiB total RAM
     ```
  2. Launch biosignal mock:
     ```bash
     python mock/biosignal_mock.py --mode signal --seed 7 --duration 3600 --port 7100
     ```
  3. Start pipeline service:
     ```bash
     sudo systemctl start pipeline
     ```
  4. Run status check:
     ```bash
     pipeline status
     # Output: {"mode": "online", "samples_saved": 4000, "samples_in_cloud": 4000, "backlog": 0}
     ```

### [0:45 - 2:00] Scenario 1: Network Cut & Offline Processing
- **Voiceover**: *"We will now simulate an abrupt hospital WiFi loss by cutting the cloud network connection."*
- **Actions**:
  1. Stop cloud server (or disconnect WiFi/Ethernet):
     ```bash
     # On cloud terminal: Ctrl+C or iptables drop
     ```
  2. Run `pipeline status` repeatedly:
     - At $t=2\text{s}$: Probing cloud...
     - At $t=4\text{s}$: Switched automatically:
       ```json
       {"mode": "offline", "samples_saved": 8000, "samples_in_cloud": 4000, "backlog": 4000}
       ```
  3. Show offline window results being calculated directly on the Pi:
     ```bash
     pipeline results --session <session_id> | tail -n 3
     # Highlights "mode": "offline" in window results
     ```
  4. Restore cloud server:
     - Within 3.5 seconds, `pipeline status` returns to `"online"`.
     - Show the backlog decreasing rapidly to 0:
       ```json
       {"mode": "online", "samples_saved": 16000, "samples_in_cloud": 16000, "backlog": 0}
       ```

### [2:00 - 3:15] Scenario 2: Sudden Process Kill (`kill -9`)
- **Voiceover**: *"Now we test program crashes by sending an uncatchable SIGKILL to the pipeline daemon."*
- **Actions**:
  1. Find PID and kill:
     ```bash
     sudo killall -9 python3
     ```
  2. Watch systemd immediately resurrect the service:
     ```bash
     systemctl status pipeline
     # Shows: Active: active (running) ... Restarted 1s ago
     ```
  3. Check pipeline status and export:
     ```bash
     pipeline export --session <session_id> --from device | grep "device_down"
     # Output shows exact outage range:
     # {"sensor": 1, "from_index": 16800, "to_index": 17279, "reason": "device_down"}
     ```
  4. Voiceover: *"Notice that the downtime was honestly recorded as device_down. No data was silently dropped."*

### [3:15 - 4:15] Scenario 3: Sudden Power Cut & Ground Truth Verification
- **Voiceover**: *"Now we simulate a hard power cut by pulling the physical power cord from the Raspberry Pi."*
- **Actions**:
  1. Physically pull power cord (or hard reset VM).
  2. Plug power cord back in; Raspberry Pi boots up.
  3. Show systemd starting `pipeline.service` automatically on boot.
  4. Run `check_ground_truth`:
     ```bash
     pipeline export --session <session_id> --from device > device_export.ndjson
     python mock/check_ground_truth.py --export-file device_export.ndjson --mode signal --seed 7
     ```
  5. Show terminal output:
     ```text
     --- Summary Verification Results ---
     Parse errors: 0
     Duplicates: 0
     Corrupt values: 0
     >>> GROUND TRUTH CHECK: PASSED (100% data integrity, 0 silent losses) <<<
     ```

### [4:15 - 4:30] Conclusion
- Summary of 100/100 points criteria met:
  - Zero silent loss
  - $\le 1$ second power cut loss bound confirmed
  - Sub-10s online/offline switching confirmed
  - Complete cloud synchronization confirmed
