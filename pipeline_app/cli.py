#!/usr/bin/env python3
"""
Pipeline CLI Entry Point
Implements Section 9 required commands:
1. pipeline status: {"mode": "online", "samples_saved": N, "samples_in_cloud": N, "backlog": N}
   When offline, samples_in_cloud is the last acknowledged count.
2. pipeline export --session ID --from device|cloud: samples then losses
3. pipeline results --session ID: window results with Section 5 outputs
4. pipeline start: runs complete daemon
"""

import argparse
import json
import os
import signal
import sys
import time
import urllib.request
from typing import Optional

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from storage import StorageEngine
from network import NetworkMonitor
from mock_client import MockClient
from sync import SyncWorker
from device_processor import DeviceProcessor


def get_active_session_id(storage: StorageEngine) -> Optional[str]:
    conn = storage.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT session_id FROM sessions ORDER BY created_at DESC LIMIT 1;")
    row = cur.fetchone()
    return row[0] if row else None


def cmd_status(args):
    storage = StorageEngine(args.db_path)
    session_id = args.session or get_active_session_id(storage)

    net = NetworkMonitor(cloud_url=args.cloud_url, timeout=1.0)
    is_online = net.probe_cloud()
    mode = "online" if is_online else "offline"

    samples_saved = 0
    samples_in_cloud = 0
    backlog = 0

    if session_id:
        total, synced, bl = storage.get_counts(session_id)
        samples_saved = total
        # Section 9: When offline, samples_in_cloud is the last acknowledged count
        samples_in_cloud = synced
        backlog = bl

        if is_online:
            try:
                url = f"{args.cloud_url.rstrip('/')}/api/v1/status?session_id={session_id}"
                with urllib.request.urlopen(url, timeout=1.5) as resp:
                    if resp.status == 200:
                        c_data = json.loads(resp.read().decode("utf-8"))
                        samples_in_cloud = c_data.get("samples_in_cloud", synced)
                        backlog = max(0, samples_saved - samples_in_cloud)
            except Exception:
                pass

    status_obj = {
        "mode": mode,
        "samples_saved": samples_saved,
        "samples_in_cloud": samples_in_cloud,
        "backlog": backlog
    }
    sys.stdout.write(json.dumps(status_obj) + "\n")
    sys.stdout.flush()


def cmd_export(args):
    if not args.session:
        sys.stderr.write("Error: --session ID is required for export\n")
        sys.exit(1)

    source = args.from_source.lower()
    if source == "device":
        storage = StorageEngine(args.db_path)
        for line in storage.export_session_lines(args.session):
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
    elif source == "cloud":
        url = f"{args.cloud_url.rstrip('/')}/api/v1/export?session_id={args.session}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PipelineExport/1.0"})
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                for line in resp:
                    sys.stdout.write(line.decode("utf-8"))
            sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Error fetching cloud export: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Error: Unknown --from source '{source}'. Must be 'device' or 'cloud'.\n")
        sys.exit(1)


def cmd_results(args):
    if not args.session:
        sys.stderr.write("Error: --session ID is required for results\n")
        sys.exit(1)

    cloud_results = {}
    try:
        url = f"{args.cloud_url.rstrip('/')}/api/v1/results?session_id={args.session}"
        req = urllib.request.Request(url, headers={"User-Agent": "PipelineResults/1.0"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            for raw in resp:
                line_str = raw.decode("utf-8").strip()
                if line_str:
                    item = json.loads(line_str)
                    cloud_results[item["window"]] = line_str
    except Exception:
        pass

    storage = StorageEngine(args.db_path)
    device_results = {}
    for line_str in storage.get_results_lines(args.session):
        item = json.loads(line_str)
        device_results[item["window"]] = line_str

    all_windows = sorted(set(list(cloud_results.keys()) + list(device_results.keys())))
    for w in all_windows:
        if w in cloud_results:
            sys.stdout.write(cloud_results[w] + "\n")
        elif w in device_results:
            sys.stdout.write(device_results[w] + "\n")
    sys.stdout.flush()


def cmd_start(args):
    print(f"[Pipeline] Initializing storage at {args.db_path}...")
    storage = StorageEngine(args.db_path)

    print(f"[Pipeline] Starting network monitor probing {args.cloud_url}...")
    net_monitor = NetworkMonitor(cloud_url=args.cloud_url)
    net_monitor.start()

    print(f"[Pipeline] Starting cloud synchronization worker...")
    sync_worker = SyncWorker(storage=storage, network_monitor=net_monitor, cloud_url=args.cloud_url)
    sync_worker.start()

    print(f"[Pipeline] Starting device offline processor...")
    dev_processor = DeviceProcessor(storage=storage, network_monitor=net_monitor)
    dev_processor.start()

    print(f"[Pipeline] Connecting ingestion client to mock on {args.mock_host}:{args.mock_port}...")
    mock_client = MockClient(host=args.mock_host, port=args.mock_port, storage=storage)

    def shutdown_handler(signum, frame):
        print("\n[Pipeline] Shutting down gracefully...")
        mock_client.stop()
        sync_worker.stop()
        dev_processor.stop()
        net_monitor.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        mock_client.run_loop()
    except Exception as e:
        print(f"[Pipeline] Fatal error: {e}")
        shutdown_handler(None, None)


def load_config() -> dict:
    # Look for config.json in root or current dir
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates = [
        os.path.join(base_dir, "config.json"),
        os.path.join(CURRENT_DIR, "config.json"),
        "config.json"
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return {}


def main():
    cfg = load_config()
    default_db = cfg.get("db_path", "pipeline.db")
    default_cloud = cfg.get("cloud_url", "http://127.0.0.1:8000")
    default_mock_host = cfg.get("mock_host", "127.0.0.1")
    default_mock_port = int(cfg.get("mock_port", 7100))

    parser = argparse.ArgumentParser(prog="pipeline", description="Never Lose a Heartbeat Data Pipeline")
    parser.add_argument("--db-path", default=default_db, help="Path to local SQLite database")
    parser.add_argument("--cloud-url", default=default_cloud, help="Cloud service URL")
    parser.add_argument("--mock-host", default=default_mock_host, help="Mock service host")
    parser.add_argument("--mock-port", type=int, default=default_mock_port, help="Mock service port")

    subparsers = parser.add_subparsers(dest="command")

    parser_status = subparsers.add_parser("status", help="Print pipeline status as single JSON line")
    parser_status.add_argument("--session", type=str, default=None, help="Session ID")

    parser_export = subparsers.add_parser("export", help="Export session data as JSON lines")
    parser_export.add_argument("--session", type=str, required=True, help="Session ID")
    parser_export.add_argument("--from", dest="from_source", choices=["device", "cloud"], default="device",
                               help="Source: 'device' or 'cloud'")

    parser_results = subparsers.add_parser("results", help="Print processed results as JSON lines")
    parser_results.add_argument("--session", type=str, required=True, help="Session ID")

    parser_start = subparsers.add_parser("start", help="Run continuous pipeline service")

    args = parser.parse_args()

    if args.command == "status":
        cmd_status(args)
    elif args.command == "export":
        cmd_export(args)
    elif args.command == "results":
        cmd_results(args)
    elif args.command == "start":
        cmd_start(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
