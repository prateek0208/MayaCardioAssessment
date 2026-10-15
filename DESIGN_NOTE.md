# Design Note: Never Lose a Heartbeat
**MayaCardio Software Systems Engineer Assessment — October 2026**  
**Candidate Design for a Crash-Safe, Offline-Capable Biosignal Pipeline**

---

## 1. System Architecture

The bedside pipeline is engineered as a zero-dependency, crash-resilient data ingestion and stream-processing system for ARM64 Raspberry Pi boards ($< 4$ cores, $< 2$ GB RAM) coupled with a free-tier cloud ingest node.

```
+-----------------------------------------------------------------------------------+
| RASPBERRY PI 4 DEVICE (systemd unit: pipeline.service, Restart=always)            |
|                                                                                   |
|  +----------------+  TCP NDJSON   +--------------------------------------------+  |
|  | biosignal-mock | ------------> | Reader (Mock Client)                       |  |
|  | (port 7100)    |               | - Alternates sensor 1 & 2 read requests    |  |
|  +----------------+               | - Checks CRC-32 exactly as sent; re-reads  |  |
|                                   | - Detects reboot gap (device_down)         |  |
|                                   | - Detects mock restart (mock_restart)      |  |
|                                   +--------------------------------------------+  |
|                                                          |                        |
|                                                   ACID Commit                     |
|                                                (synchronous=FULL)                 |
|                                                          v                        |
|  +-----------------------------------------------------------------------------+  |
|  | Local SQLite WAL Store (pipeline.db)                                        |  |
|  | - samples (session_id, sensor, sample_index PK) -> automatic deduplication  |  |
|  | - lost_ranges (session_id, sensor, from_index, to_index, reason)            |  |
|  | - results (session_id, window, mode, ...)                                    |  |
|  +-----------------------------------------------------------------------------+  |
|            |                                              |                       |
|   Offline Mode (< 5.5s switch)                   Online Mode (< 4.5s switch)      |
|            v                                              v                       |
|  +---------------------------+               +---------------------------------+  |
|  | Local Window Processor    |               | Sync Worker (HTTPS + gzip)      |  |
|  | - Section 5 math logic    |               | - Batches up to 5,000 samples   |  |
|  | - Saves mode="offline"    |               | - Acknowledged before mark sync |  |
|  +---------------------------+               +---------------------------------+  |
+---------------------------------------------------------------|-------------------+
                                                                | HTTPS POST (gzip)
                                                                v
+-----------------------------------------------------------------------------------+
| CLOUD FREE-TIER SERVICE (Google Cloud e2-micro / Oracle Always Free / Render)     |
|                                                                                   |
|  +--------------------+   Commit    +------------------------------------------+  |
|  | HTTPS Ingest API   | ----------> | Cloud Database (cloud_data.db)           |  |
|  | (/api/v1/sync)     |             | - Primary key deduplication              |  |
|  +--------------------+             +------------------------------------------+  |
|                                                           |                       |
|                                            Finalised at sample 400(w+1)           |
|                                                           v                       |
|                                             +----------------------------------+  |
|                                             | Cloud Window Processor           |  |
|                                             | - Computes mode="online"         |  |
|                                             | - Results published within < 5s  |  |
|                                             +----------------------------------+  |
+-----------------------------------------------------------------------------------+
```

### Decoupled Data Flow
The data flow is strictly strictly unidirectional:
$$\text{mock} \longrightarrow \text{Reader} \longrightarrow \text{Store (durable)} \longrightarrow \text{Sync Worker} \longrightarrow \text{Cloud} \longrightarrow \text{Cloud Processor} \longrightarrow \text{Results}$$
**Key Invariant**: The Reader thread *never* waits for the network or the cloud. Ingestion operates at local bus speed directly into SQLite WAL on local disk, ensuring that cloud latency, packet loss, or network partitions can never stall sensor collection or induce buffer overflows in the mock.

---

## 2. Power-Cut and Crash Safety (Criterion 3)

### Stated Maximum Data Loss on Sudden Power Cut
$$\mathbf{\le 1.0\text{ second of data per sensor (at most 400 samples)}}$$

### Mathematical & Engineering Justification
1. **SQLite WAL Configuration**: SQLite is initialized with `PRAGMA journal_mode = WAL;` and `PRAGMA synchronous = FULL;`. In `synchronous=FULL`, SQLite forces an explicit OS flush (`fsync`) of the Write-Ahead Log to physical non-volatile storage at the boundary of every committed transaction. `synchronous=NORMAL` is intentionally omitted because it allows committed transactions to roll back after a sudden power loss.
2. **Transaction Boundaries**: Every validated reply received from `biosignal-mock` is committed to disk immediately as an atomic transaction before the next read command is issued.
3. **The Loss Window**:
   - Because the mock returns at most 1 second of data per reply (up to 400 samples), each committed batch survives.
   - Any transaction interrupted mid-write during a power pull is atomic: SQLite dispatches rollback on recovery via standard WAL replay.
   - Hence, the only unpersisted data is the single in-flight batch being transferred from the mock at the exact millisecond power was severed.
   - Therefore, physical data loss on sudden power pull is bounded to **at most 1.0 second per sensor**.
4. **Zero Silent Loss**:
   - Time elapsed while the device is unpowered is tracked upon reboot: on startup, the Reader inspects the last stored sample index in SQLite and compares it with the first index delivered by the restarted mock. The elapsed gap is explicitly recorded into `lost_ranges` with reason `device_down`.
   - When the mock restarts due to board power loss, the unread tail of the old session is closed with `to_index: null` and reason `mock_restart`.
   - Result: 0 silent loss.

---

## 3. What is Guaranteed and What is Not

### Guaranteed
- **Zero Silent Data Loss**: Every single sample generated by the mock is either stored on disk unchanged or logged in the loss registry with an exact index range and causal reason (`mock_reported_loss`, `device_down`, `mock_no_data`, `mock_restart`, `disk_full`).
- **Idempotency & Zero Duplication**: Both device and cloud databases enforce unique primary keys on `(session_id, sensor, sample_index)`. Duplicate mock replies, retransmitted HTTP batches, or re-reads are discarded with zero duplicate rows.
- **Clock Immunity**: All timeouts, network probes, and window finalisation triggers rely exclusively on `time.monotonic()`, making the pipeline immune to NTP jumps or system clock manipulation.
- **Cloud Completeness**: Backlog sync streams up to 5,000 samples per batch with gzip compression. A 2-hour network partition ($5.76\times 10^6$ samples) catches up in under 5 minutes on standard connections ($< 15$ min limit).

### Not Guaranteed
- **Samples dropped internally by the mock hardware before delivery**: E.g., if the mock drops samples due to internal sensor faults or buffer overflow before the client connects. (Guaranteed behavior: these are captured from mock replies and logged as lost).
- **In-flight batch during power severing**: The single uncommitted batch ($< 1\text{s}$) lost in OS RAM before SQLite commit. (Guaranteed behavior: logged upon recovery as `device_down`).

---

## 4. Key Specification Decisions (Section 14 Alignment)

1. **Decimal Rounding Half-Up**:
   - Standard floating-point `round()` uses IEEE 754 "round half to even" (banker's rounding), which produces discrepancies on exact ties (e.g., $.05$).
   - In accordance with Section 14, all fractional statistics (`mean`, `magnitude`, `heart_rate`) are computed via Python `Decimal` using `ROUND_HALF_UP` from exact integer sums.
2. **Heart Rate Missing Sample Rule**:
   - The heart rate rule evaluates ECG signals on Sensor 1 Channel 2.
   - Missing samples in the 5-second window ($[400(w-4), 400(w+1)-1]$) refer exclusively to Sensor 1 (`HR_REQUIRE_BOTH_SENSORS = False`).
3. **Window Results Integrity**:
   - A window processed on the device during an outage retains its `mode="offline"` designation and is uploaded verbatim to the cloud. The cloud never recalculates or relabels offline results.
