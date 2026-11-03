#!/usr/bin/env python3
"""A reference consumer for the stinger socket.

Connects to the unix socket hived serves the match stream on (retrying
until it appears), checks the greeting, and prints one line per record.
The record format is documented in ../include/hive/plugins/stinger/records.hpp.

    consumer.py /path/to/stinger.sock            one readable line per record
    consumer.py /path/to/stinger.sock --testnet  the same, with TESTS/TBD for the testnet's assets
    consumer.py /path/to/stinger.sock --raw      the JSON lines as received
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time

FORMAT_VERSION = 1

# asset symbols by numeric asset identifier; a testnet calls the first two TESTS and TBD
NAI = {"@@000000021": "HIVE", "@@000000013": "HBD", "@@000000037": "VESTS"}
TESTNET_NAI = {"@@000000021": "TESTS", "@@000000013": "TBD", "@@000000037": "VESTS"}
symbols = NAI


def connect(path: str, timeout: float) -> socket.socket:
    deadline = time.monotonic() + timeout
    while True:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect(path)
            return s
        except OSError:
            s.close()
            if time.monotonic() > deadline:
                sys.exit(f"consumer: no socket at {path} after {timeout:.0f}s")
            time.sleep(0.2)


def fmt_value(v: object) -> str:
    if isinstance(v, dict) and set(v) == {"amount", "precision", "nai"}:
        amount, precision = int(v["amount"]), int(v["precision"])
        whole, frac = divmod(amount, 10**precision)
        number = f"{whole}.{frac:0{precision}d}" if precision else str(whole)
        return f"{number} {symbols.get(v['nai'], v['nai'])}"
    text = v if isinstance(v, str) else json.dumps(v, separators=(",", ":"))
    return text if len(text) <= 40 else text[:37] + "..."


def describe_op(op: object, omitted: object) -> str:
    if not isinstance(op, dict):
        return f"(op omitted: {omitted})"
    value = op.get("value", {})
    fields = " ".join(f"{k}={fmt_value(v)}" for k, v in value.items())
    return f"{op.get('type', '?').removesuffix('_operation')} {fields}"


def describe(rec: dict) -> str:
    kind = rec.get("kind", "?")
    if kind == "hello":
        delivered = rec.get("delivered", {})
        head = delivered.get("head")
        head_s = f"#{head['block']}" if head else "-"
        irr = delivered.get("irreversible")
        irr_s = f"#{irr}" if irr is not None else "-"
        return f"{kind:14} version={rec.get('version')} last_seq={rec.get('seq')} head={head_s} irreversible={irr_s}"
    block = f"#{rec.get('block', '?')}"
    if kind == "match":
        rules = ",".join(rec.get("rules", []))
        body = describe_op(rec.get("op"), rec.get("op_omitted"))
        return f"{kind:14} {block:>7} {rec.get('delivery', ''):13} {body}  [{rules}]"
    if kind == "dropped":
        return (
            f"{kind:14} {'':>7} {rec.get('delivery', ''):13} "
            f"{rec.get('blocks')} blocks between #{rec.get('from_block')} and #{rec.get('to_block')} lost"
        )
    # block_applied, irreversible, switch_fork
    block_id = rec.get("block_id")
    return f"{kind:14} {block:>7} {block_id[:16] + '...' if block_id else ''}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("socket", help="path of the unix socket (stinger-socket in the node's config)")
    ap.add_argument("--raw", action="store_true", help="print the JSON lines as received")
    ap.add_argument("--testnet", action="store_true", help="name the assets TESTS and TBD, as a testnet does")
    ap.add_argument("--connect-timeout", type=float, default=60.0, help="seconds to wait for the socket to appear")
    args = ap.parse_args()
    global symbols
    if args.testnet:
        symbols = TESTNET_NAI

    s = connect(args.socket, args.connect_timeout)
    expected_seq = None
    with s.makefile("rb") as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                # the node went away mid-record (killed hard); nothing to parse
                break
            rec = json.loads(line)
            if rec.get("kind") == "hello":
                if rec.get("version") != FORMAT_VERSION:
                    sys.exit(f"consumer: stream format version {rec.get('version')}, this consumer knows {FORMAT_VERSION}")
                expected_seq = rec["seq"] + 1
            else:
                seq = rec.get("seq")
                if expected_seq is not None and seq != expected_seq:
                    print(f"{'(gap)':14} expected seq {expected_seq}, got {seq}", flush=True)
                expected_seq = seq + 1
            if args.raw:
                sys.stdout.write(line.decode("utf-8", "replace"))
                sys.stdout.flush()
            else:
                print(describe(rec), flush=True)
    print("consumer: node closed the socket", flush=True)


if __name__ == "__main__":
    main()
