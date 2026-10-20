#!/usr/bin/env python3
import argparse
import random
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

from proto import NetworkRequests_pb2 as NetworkRequests
from proto.UnitDefinition_pb2 import TcpPackageType, UnitType
from utils import client as tcp_client
from utils import msg
from utils.query_result import QueryResult, results_to_dataframe
from utils.workload_executor import (
    AVAILABLE_QUERIES,
    InFlightTracker,
    SequenceCounter,
    _build_query,
    _configure_workers,
    _discover_compute_unit,
    _tracked_run_query,
    complete_plans,
    plan_finished_cb,
    update_uuids,
)

ALL_SSB_QUERIES = [
    "q_1_1", "q_1_2", "q_1_3",
    "q_2_1", "q_2_2", "q_2_3",
    "q_3_1", "q_3_2", "q_3_3", "q_3_4",
    "q_4_1", "q_4_2", "q_4_3",
]

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


def send_server_configuration(
        client: tcp_client.TCPClient,
        callback_fn: Optional[int] = None,
        callback_pkg_type: Optional[TcpPackageType] = None,
        window_size: Optional[float] = None,
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
        wait_for_config_reply(client)


def run_random_load(
        client: tcp_client.TCPClient,
        queries: list[str],
        scale_factor: int,
        groups: int,
        max_group_size: int,
        workers: int,
        window_size: float,
        threshold: Optional[int] = None,
        max_overhead: Optional[float] = None,
) -> list[QueryResult]:
    src_uuid, tgt_uuid = _discover_compute_unit(client)
    _configure_workers(client, src_uuid, workers)

    send_server_configuration(
        client=client,
        callback_fn=NetworkRequests.CallbackFunction.QUERY_PLAN_CB,
        callback_pkg_type=TcpPackageType.QUERY_PLAN,
        window_size=window_size,
        threshold=threshold,
        maxOverhead=max_overhead,
        wait_ack=True,
        src_uuid=src_uuid,
        tgt_uuid=tgt_uuid,
    )
    time.sleep(1)

    tracker = InFlightTracker()
    seq = SequenceCounter()
    results: list[QueryResult] = []
    window_s = window_size / 1000.0

    for group in range(1, groups + 1):
        num_queries = random.randint(1, max_group_size)

        for i in range(num_queries):
            query_name = random.choice(queries)
            query_msg = _build_query(query_name, src_uuid, tgt_uuid, scale_factor)
            submit_ts = datetime.now().timestamp()
            seq_num = seq.next()

            res = _tracked_run_query(
                client=client,
                query=query_msg,
                query_name=query_name,
                mode="random_load",
                scale_factor=scale_factor,
                iteration=group,
                workers=workers,
                tracker=tracker,
                workload_name="random_load",
                phase_name=None,
                user_id=None,
                sequence_number=seq_num,
                planned_delay_s=0,
                submit_timestamp=submit_ts,
            )
            res.window_size = window_size
            if max_overhead is not None:
                res.max_overhead = max_overhead

            results.append(res)
            complete_plans.clear()

            if i < num_queries - 1:
                time.sleep(random.uniform(0.001, window_s / num_queries))

        time.sleep(window_s * 1.2)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", default="127.0.0.1")
    parser.add_argument("-port", type=int, default=23232)
    parser.add_argument("-sf", "--scale-factor", type=int, default=10)
    parser.add_argument("-g", "--groups", type=int, default=20)
    parser.add_argument("-m", "--max-group-size", type=int, default=10)
    parser.add_argument("-w", "--workers", type=int, default=1)
    parser.add_argument("-q", "--queries", nargs="+", default=ALL_SSB_QUERIES)
    parser.add_argument("--window-size", type=float, required=True)
    parser.add_argument("--threshold", type=int, default=10)
    parser.add_argument("--max-overhead", type=float, default=2.0)
    parser.add_argument("-o", "--output", default=None)
    args = parser.parse_args()

    for q in args.queries:
        if q not in AVAILABLE_QUERIES:
            sys.exit(1)

    client = tcp_client.TCPClient(
        unit_type=UnitType.QUERY_PLANER,
        unit_info="Random Load Gen",
        name="Random Load Gen",
    )
    client.register_callback(TcpPackageType.UUID_FOR_UNIT_RESPONSE, update_uuids)
    client.register_callback(TcpPackageType.PLAN_RESPONSE, plan_finished_cb)
    client.register_callback(TcpPackageType.SERVER_CONFIGURATION_RESPONSE, server_config_response_cb)

    client.connect(args.ip, args.port)

    try:
        results = run_random_load(
            client=client,
            queries=args.queries,
            scale_factor=args.scale_factor,
            groups=args.groups,
            max_group_size=args.max_group_size,
            workers=args.workers,
            window_size=args.window_size,
            threshold=args.threshold,
            max_overhead=args.max_overhead,
        )

        out_path = args.output or f"results/random_load_{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.csv"
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)

        df: pd.DataFrame = results_to_dataframe(results)
        df.to_csv(out_path, index=False)

    finally:
        client.disconnect()


if __name__ == "__main__":
    main()