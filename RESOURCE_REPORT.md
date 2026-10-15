# 1-Hour Resource Consumption Report
**MayaCardio Software Systems Engineer Assessment — October 2026**  
**Criterion 8 Compliance: Small-Device Limits (ARM64, $\le 4$ cores, $\le 2$ GB RAM)**

---

## 1. Hardware & Operating Environment

- **Board**: Raspberry Pi 4 Model B (Revision 1.4)
- **Architecture**: ARM64 (`aarch64`)
- **CPU**: Broadcom BCM2711, Quad-core Cortex-A72 @ 1.5 GHz (4 cores total)
- **RAM**: 2 GB LPDDR4-3200 SDRAM
- **Storage**: 32 GB SanDisk Extreme Class 10 MicroSD (ext4 filesystem, `noatime`)
- **Operating System**: Raspberry Pi OS (Debian 12 Bookworm, 64-bit kernel 6.6)
- **Resource Enforcement**: Systemd slice limits:
  ```ini
  CPUQuota=400%
  MemoryMax=2G
  ```
- **Measurement Instrumentation**:
  - `pidstat -u -r -d 1`: Per-second CPU, virtual memory, RSS, and disk write I/O.
  - `iostat -xz 1`: Block device queue depth and storage sector write rate.
  - `/proc/net/dev`: Network socket and interface byte counters (wlan0 / eth0).

---

## 2. Resource Measurements over 1 Hour (Online Mode)

Test configuration:
- `biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100` (runs concurrently on the same board)
- `pipeline` daemon continuously reading both sensors at 400 sps ($2 \times 400 = 800\text{ samples/sec}$)
- Streaming gzip-compressed HTTPS batches to cloud endpoint once per second
- Total test duration: **3,600 seconds (1.0 hour)**
- Total samples recorded: **$2,880,000$ samples** ($1,440,000$ per sensor)

| Metric | Permitted Budget | Measured Value (Pipeline Only) | Measured Value (Board Total: Pipeline + Mock) | Headroom / Margin |
|---|---|---|---|---|
| **Average CPU Utilization** | $\le 400.0\%$ (4 Cores) | **2.85%** | **4.62%** | **98.8% Headroom** |
| **Peak Memory (RSS)** | $\le 2048.0\text{ MB}$ (2 GB) | **38.4 MB** | **64.2 MB** | **96.8% Headroom** |
| **Megabytes Written to Disk** | Unconstrained (low write profile) | **78.2 MB** | **84.6 MB** | **$\approx 22\text{ KB/sec}$** |
| **Megabytes Sent over Network** | Unconstrained (cloud ingest) | **14.1 MB** (gzip) | **14.1 MB** | **$\approx 3.9\text{ KB/sec}$** |

---

## 3. Analysis & Optimization Details

1. **CPU Efficiency (2.85%)**:
   - The Reader, SQLite storage, and sync worker are asynchronous, non-blocking, and I/O-driven.
   - Batching samples (1-second chunks from mock) eliminates per-sample function call overhead.
   - Mathematical calculations (mean, min, max, magnitude, peak detection) execute in pure C-level Python primitives and Decimal integer arithmetic.
2. **Memory Footprint (38.4 MB Peak RSS)**:
   - SQLite cache is explicitly configured (`PRAGMA cache_size = -64000;`, 64 MB maximum cache ceiling).
   - Python processes samples in streaming generators (`export_session_lines`), preventing high-memory list allocations.
   - Far below the 2 GB hardware limit.
3. **Disk Write Optimization (78.2 MB / hour)**:
   - SQLite WAL mode commits transactions with sequential journal writes instead of random page updates.
   - Automatic checkpointing keeps the `-wal` file small ($< 4\text{ MB}$).
   - 78.2 MB written per hour translates to $< 22\text{ KB/sec}$, preserving SD card flash endurance for years of 24/7 hospital deployment.
4. **Network Bandwidth (14.1 MB / hour)**:
   - JSON payloads are compressed with `gzip` on-the-fly (`Content-Encoding: gzip`).
   - Achieves an $82\%$ compression ratio on biosignal numbers.
   - $14.1\text{ MB/hour} \approx 3.9\text{ KB/sec} \approx 31.2\text{ kbps}$, effortlessly transmitting through congested hospital WiFi networks.
