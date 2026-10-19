#!/usr/bin/env python3
"""
Run OLAP workloads defined in YAML configuration files with two phases:
  Phase 1: FORWARD_CB   (baseline)
  Phase 2: QUERY_PLAN_CB (with window size + threshold)

Usage:
    # Single config
    python run_workload.py -config configs/workload_mix.yaml

    # Multiple configs in sequence
    python run_workload.py -config configs/workload_mix.yaml configs/workload_phases.yaml

    # Override connection parameters
    python run_workload.py -config configs/workload_mix.yaml -ip 10.0.0.5 -port 23232

    # Customize threshold / window
    python run_workload.py -config configs/workload_mix.yaml --threshold 10 --window-size 100

    # Dry run (validate config without executing)
    python run_workload.py -config configs/workload_mix.yaml -dry-run
"""

import argparse
import sys
import threading
import time
from pathlib import Path
from datetime import datetime
from typing import List, Optional

import pandas as pd

from utils import msg
from utils import client as tcp_client
from utils import util

from proto import NetworkRequests_pb2 as NetworkRequests
from proto.UnitDefinition_pb2 import TcpPackageType, UnitType

from utils.workload_config import load_workload_config, validate_config
from utils.workload_executor import (
    execute_workload,
    update_uuids,
    plan_finished_cb,
    AVAILABLE_QUERIES,
    # Re-use executor's global state so CU discovery is shared
    name_list as executor_name_list,
    uuid_list as executor_uuid_list,
    uuid_condition as executor_uuid_condition,
)
from utils.query_result import results_to_dataframe


# ----------------------------
# Server config reply state (not handled by executor)
# ----------------------------
config_replies: List[bool] = []
config_condition = threading.Condition()


def server_config_response_cb(message: msg.TCPMessage):
    resp = NetworkRequests.ServerConfigurationResponse()
    resp.ParseFromString(message.payload)
    with config_condition:
        config_replies.append(bool(resp.success))
        config_condition.notify_all()


def wait_for_config_reply(client: tcp_client.TCPClient) -> Optional[bool]:
    with config_condition:
        while len(config_replies) == 0 and client.connection_up:
            config_condition.wait()
        if len(config_replies) > 0:
            return config_replies.pop(0)
    return None


# ----------------------------
# Server configuration helper
# ----------------------------
def send_server_configuration(
    client: tcp_client.TCPClient,
    callback_fn: Optional[int] = None,
    callback_pkg_type: Optional[TcpPackageType] = None,
    window_size: Optional[int] = None,
    threshold: Optional[int] = None,
    maxOverhead: Optional[float] = None,
    wait_ack: bool = True,
    src_uuid: int = 0,
    tgt_uuid: int = 0,
):
    req = NetworkRequests.ServerConfigurationRequest()

    if callback_fn is not None:
        cb = NetworkRequests.CallbackChangeRequest()
        cb.packageType = int(callback_pkg_type) if callback_pkg_type is not None else int(TcpPackageType.QUERY_PLAN)
        cb.callbackFunction = int(callback_fn)
        req.callbackChange.CopyFrom(cb)

    if window_size is not None:
        req.windowSizeMs = int(window_size)

    if threshold is not None:
        req.threshold = int(threshold)

    if maxOverhead is not None:
        req.maxOverhead = float(maxOverhead)

    msg_out = msg.TCPMessage(
        unit_type=UnitType.QUERY_PLANER,
        package_type=TcpPackageType.SERVER_CONFIGURATION,
        src_uuid=src_uuid,
        tgt_uuid=tgt_uuid,
    )
    msg_out.payload = req.SerializeToString()
    client.send_message(msg_out)

    if wait_ack:
        ok = wait_for_config_reply(client)
        if ok is None:
            print("[Warn] No ServerConfigurationResponse received (continuing).")
        elif not ok:
            print("[Warn] ServerConfigurationResponse.success == False.")


def _resolve_compute_unit(client: tcp_client.TCPClient) -> tuple[int, int]:
    """
    Resolve src/tgt UUIDs from the executor's global state.
    If the executor hasn't discovered them yet, trigger a discovery.
    """
    with executor_uuid_condition:
        if len(executor_uuid_list) == 0:
            # Trigger discovery — executor will do this too, but we may
            # need UUIDs before the first execute_workload call.
            work = util.create_uuid_request_item(type=UnitType.COMPUTE_UNIT)
            client.send_message(work)
            if len(executor_uuid_list) == 0 and client.connection_up:
                executor_uuid_condition.wait(timeout=5.0)

    tgt_uuid = 0
    for name, uuid in zip(executor_name_list, executor_uuid_list):
        if "ComputeUnit" in name:
            tgt_uuid = uuid
            break

    if tgt_uuid == 0:
        raise RuntimeError("Could not find a ComputeUnit.")

    return client.uuid, tgt_uuid


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Execute OLAP workloads from YAML configurations with FORWARD / QUERY_PLAN phases.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-config", nargs="+", required=True, help="Path(s) to YAML workload config file(s).")
    parser.add_argument("-ip", default="127.0.0.1", help="Message bus IP (default: 127.0.0.1)")
    parser.add_argument("-port", type=int, default=23232, help="Message bus port (default: 23232)")
    parser.add_argument("-name", default="Workload Runner", help="Unit name for registration")
    parser.add_argument("-info", default="YAML Workload Runner", help="Unit info string")
    parser.add_argument("-dry-run", action="store_true", default=False, help="Validate configs and print summary without executing.")
    parser.add_argument("-o", "--output", default=None, help="Override output CSV path.")
    # Phase-2 knobs (QUERY_PLAN_CB)
    parser.add_argument("--threshold", type=int, default=10, help="Threshold for QUERY_PLAN_CB phase (default: 10)")
    parser.add_argument("--window-size", type=float, default=100, help="Window size (ms) for QUERY_PLAN_CB phase (default: 100)")
    parser.add_argument("--max-overhead", type=float, default=2.0, help="Max overhead for QUERY_PLAN_CB phase (default: 2.0)")
    args = parser.parse_args()

    start_time = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

    # ---- Load and validate all configs upfront ----
    configs = []
    for path in args.config:
        try:
            cfg = load_workload_config(path)
            validate_config(cfg, AVAILABLE_QUERIES)
            configs.append((path, cfg))
            print(f"[OK] Loaded: {path} → '{cfg.name}' "
                  f"(pattern={cfg.pattern.type}, sf={cfg.scale_factor}, "
                  f"workers={cfg.workers}, iters={cfg.iterations})")
        except Exception as e:
            print(f"[ERROR] Failed to load {path}: {e}", file=sys.stderr)
            sys.exit(1)

    if args.dry_run:
        print("\n[dry-run] All configs valid. Exiting.")
        return

    # ---- Connect ----
    client = tcp_client.TCPClient(unit_type=UnitType.QUERY_PLANER, unit_info=args.info, name=args.name)
    print("Client initialized!")

    # Register callbacks
    client.register_callback(TcpPackageType.UUID_FOR_UNIT_RESPONSE, update_uuids)
    client.register_callback(TcpPackageType.PLAN_RESPONSE, plan_finished_cb)
    client.register_callback(TcpPackageType.SERVER_CONFIGURATION_RESPONSE, server_config_response_cb)

    print("Connecting...")
    client.connect(args.ip, args.port)
    print("Connection established!")

    all_results = []

    try:
        for path, cfg in configs:
            print(f"\n{'#'*60}")
            print(f"# Workload: {cfg.name} ({path})")
            print(f"{'#'*60}")

            # ==============================================
            # Phase 1: FORWARD_CB (baseline)
            # ==============================================
            print("\n=== Phase 1: FORWARD_CB ===")

            # Resolve UUIDs (triggers discovery if needed; the executor's
            # update_uuids callback populates the shared lists).
            src_uuid, tgt_uuid = _resolve_compute_unit(client)

            send_server_configuration(
                client,
                callback_fn=NetworkRequests.CallbackFunction.FORWARD_CB,
                callback_pkg_type=TcpPackageType.QUERY_PLAN,
                wait_ack=True,
                src_uuid=src_uuid,
                tgt_uuid=tgt_uuid,
            )
            time.sleep(1)

            forward_results = execute_workload(client, cfg)
            # Tag every result so we can distinguish phases in analysis
            for r in forward_results:
                r.mode = f"forward/{r.mode}"
                r.window_size = 0
                r.max_overhead = 0.0
            all_results.extend(forward_results)

            print(f"  [forward] {len(forward_results)} results collected")
            time.sleep(3)

            # ==============================================
            # Phase 2: QUERY_PLAN_CB (with threshold/window)
            # =============================================
            # UUIDs guaranteed populated after first execute_workload
            src_uuid, tgt_uuid = _resolve_compute_unit(client)
            
            all_query_plan_results = []
            
            for window_size in [100, 500, 1000, 2000, 3000, 4000, 5000]:
                print(f"\n=== Phase 2: QUERY_PLAN_CB (threshold={args.threshold}, window={window_size}) ===")

                send_server_configuration(
                    client,
                    callback_fn=NetworkRequests.CallbackFunction.QUERY_PLAN_CB,
                    callback_pkg_type=TcpPackageType.QUERY_PLAN,
                    window_size=window_size,
                    threshold=args.threshold,
                    maxOverhead=args.max_overhead,
                    wait_ack=True,
                    src_uuid=src_uuid,
                    tgt_uuid=tgt_uuid,
                )
                time.sleep(1)

                query_plan_results = execute_workload(client, cfg)
                for r in query_plan_results:
                    r.mode = f"query_plan/{r.mode}"
                    r.window_size = window_size
                    r.max_overhead = args.max_overhead
                all_results.extend(query_plan_results)
                all_query_plan_results.extend(query_plan_results)

                print(f"  [query_plan] {len(query_plan_results)} results collected")

            # ---- Save per-config results (both phases) ----
            config_results = forward_results + all_query_plan_results
            output_path = args.output or cfg.output_file
            if output_path is None:
                output_path = f"results/wl/results_{cfg.name}.csv"

            output_path = output_path.replace(
                output_path.split("/")[-1],
                f"{start_time}_{output_path.split('/')[-1]}",
            )

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            df: pd.DataFrame = results_to_dataframe(config_results)
            df.to_csv(output_path, index=False)
            print(f"\n[saved] {len(config_results)} results → {output_path}")

            # Cooldown between configs
            if len(configs) > 1:
                time.sleep(3)

        # ---- Combined output if multiple configs ----
        if len(configs) > 1:
            combined_path = args.output or "results/wl/results_combined.csv"
            combined_path = combined_path.replace(
                combined_path.split("/")[-1],
                f"{start_time}_{combined_path.split('/')[-1]}",
            )
            Path(combined_path).parent.mkdir(parents=True, exist_ok=True)
            df_all: pd.DataFrame = results_to_dataframe(all_results)
            df_all.to_csv(combined_path, index=False)
            print(f"\n[saved] Combined: {len(all_results)} results → {combined_path}")

    finally:
        client.disconnect()

    print("\n[done] All workloads completed.")


if __name__ == "__main__":
    main()