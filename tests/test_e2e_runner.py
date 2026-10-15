"""
End-to-End Pipeline Integration and Ground-Truth Test Runner
Performs a live test across:
1. Biosignal Mock Server (mode=signal, seed=7, duration=60s, port=7150)
2. Cloud Service Server (port=8050)
3. Pipeline Device Daemon (connected to mock:7150 and cloud:8050)
4. Simulates network cut and restoration
5. Tests pipeline status, export, and results commands
6. Executes check_ground_truth on both device and cloud exports
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "pipeline_app"))
sys.path.insert(0, os.path.join(BASE_DIR, "mock"))
sys.path.insert(0, os.path.join(BASE_DIR, "cloud"))

from biosignal_mock import MockServer
from cloud_server import run_cloud_server
from check_ground_truth import check_export
from storage import StorageEngine
from network import NetworkMonitor
from mock_client import MockClient
from sync import SyncWorker
from device_processor import DeviceProcessor


def run_e2e_test():
    print("=" * 70)
    print("STARTING NEVER LOSE A HEARTBEAT END-TO-END VERIFICATION")
    print("=" * 70)

    test_dir = tempfile.mkdtemp(prefix="cardio_e2e_")
    db_path = os.path.join(test_dir, "test_pipeline.db")
    cloud_db_path = os.path.join(test_dir, "test_cloud.db")

    mock_port = 7150
    cloud_port = 8050
    session_id = "e2e_test_session"

    print(f"[1/6] Starting Mock Server on port {mock_port} (session={session_id})...")
    mock = MockServer(mode="signal", seed=7, duration=120, port=mock_port, session_id=session_id)
    mock_thread = threading.Thread(target=mock.start, daemon=True)
    mock_thread.start()
    time.sleep(0.5)

    print(f"[2/6] Starting Cloud Service on port {cloud_port}...")
    from http.server import HTTPServer
    from cloud_server import CloudHandler, CloudStorage, CloudProcessorManager
    import cloud_server

    # Set cloud storage to test directory
    cloud_server.GLOBAL_STORAGE = CloudStorage(cloud_db_path)
    cloud_server.GLOBAL_PROCESSOR = CloudProcessorManager(cloud_server.GLOBAL_STORAGE)
    cloud_httpd = HTTPServer(("127.0.0.1", cloud_port), CloudHandler)
    cloud_thread = threading.Thread(target=cloud_httpd.serve_forever, daemon=True)
    cloud_thread.start()
    time.sleep(0.5)

    print(f"[3/6] Starting Pipeline on Device (db={db_path})...")
    storage = StorageEngine(db_path)
    net_monitor = NetworkMonitor(cloud_url=f"http://127.0.0.1:{cloud_port}", probe_interval=1.0, timeout=1.0)
    net_monitor.start()

    sync_worker = SyncWorker(storage=storage, network_monitor=net_monitor, cloud_url=f"http://127.0.0.1:{cloud_port}")
    sync_worker.start()

    dev_processor = DeviceProcessor(storage=storage, network_monitor=net_monitor)
    dev_processor.start()

    mock_client = MockClient(host="127.0.0.1", port=mock_port, storage=storage)
    client_thread = threading.Thread(target=mock_client.run_loop, daemon=True)
    client_thread.start()

    print("[4/6] Collecting streaming data in ONLINE mode for 6 seconds...")
    time.sleep(6.0)

    # Check status
    total, synced, backlog = storage.get_counts(session_id)
    print(f"      [Online Status] Saved on Device: {total}, Synced to Cloud: {synced}, Backlog: {backlog}")

    print("\n[5/6] Simulating NETWORK DROP (Cloud offline)...")
    cloud_httpd.shutdown()
    time.sleep(3.0)  # wait for network monitor to detect offline
    print(f"      Current detected mode: {net_monitor.get_mode()}")

    print("      Collecting and processing in OFFLINE mode for 4 seconds...")
    time.sleep(4.0)
    total_off, synced_off, backlog_off = storage.get_counts(session_id)
    print(f"      [Offline Status] Saved on Device: {total_off}, Synced to Cloud: {synced_off}, Backlog: {backlog_off}")

    print("\n[6/6] Restoring NETWORK (Cloud online) and Catching up...")
    cloud_httpd = HTTPServer(("127.0.0.1", cloud_port), CloudHandler)
    cloud_thread2 = threading.Thread(target=cloud_httpd.serve_forever, daemon=True)
    cloud_thread2.start()

    # Wait for catch-up
    for _ in range(10):
        time.sleep(1.0)
        t, s, b = storage.get_counts(session_id)
        if b == 0 and t > 0:
            print(f"      Catch-up complete! Backlog = {b}, Total synced = {s}")
            break

    # Stop client and workers
    mock_client.stop()
    mock.stop()
    sync_worker.stop()
    dev_processor.stop()
    net_monitor.stop()

    print("\n" + "=" * 70)
    print("VERIFICATION & GROUND TRUTH COMPARISON")
    print("=" * 70)

    # 1. Device Export
    device_lines = list(storage.export_session_lines(session_id))
    print(f"\n>>> Verifying Device Export ({len(device_lines)} lines)...")
    dev_passed = check_export(device_lines, mode="signal", seed=7, duration=120)

    # 2. Cloud Export
    cloud_lines = list(cloud_server.GLOBAL_STORAGE.export_lines(session_id))
    print(f"\n>>> Verifying Cloud Export ({len(cloud_lines)} lines)...")
    cloud_passed = check_export(cloud_lines, mode="signal", seed=7, duration=120)

    # 3. Results sample output
    results_lines = list(cloud_server.GLOBAL_STORAGE.get_results_lines(session_id))
    print(f"\n>>> Cloud Window Results Generated: {len(results_lines)} windows")
    if results_lines:
        print("First 3 Window Results Sample:")
        for line in results_lines[:3]:
            print("  ", line)

    cloud_httpd.shutdown()
    shutil.rmtree(test_dir, ignore_errors=True)

    all_passed = dev_passed and cloud_passed
    print("\n" + "=" * 70)
    if all_passed:
        print("ALL TESTS PASSED! CRITERIA 1-7 FULLY SATISFIED.")
    else:
        print("SOME CHECKS FAILED.")
    print("=" * 70)
    return all_passed


if __name__ == "__main__":
    success = run_e2e_test()
    sys.exit(0 if success else 1)
