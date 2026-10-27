"""Helpers for driving the stinger plugin from functional tests: enabling it on a node and reading its socket."""

from __future__ import annotations

import contextlib
import json
import socket
import threading
import time
from typing import TYPE_CHECKING, Any, Callable

import msgspec

import test_tools as tt
from test_tools.__private.node import Node
from test_tools.__private.process.node_config import NodeConfig
from test_tools.__private.user_handles.get_implementation import get_implementation

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

Record = dict[str, Any]


class StingerNodeConfig(NodeConfig):
    """NodeConfig with the stinger plugin's options; test-tools does not know them yet."""

    stinger_rules_file: str | None = None
    stinger_socket: str | None = None
    stinger_match_during_sync: bool | None = None


# NodeConfig resolves a member's type through the concrete class's own annotations, which do not include the
# parent's, so the parent's are merged in
StingerNodeConfig.__annotations__ = {**NodeConfig.__annotations__, **StingerNodeConfig.__annotations__}


def rule(name: str, match: str, *, exec_type: str = "socket", delivery: str | None = None) -> dict[str, Any]:
    """One entry of a stinger rules file."""
    entry: dict[str, Any] = {"name": name, "match": match, "exec": {"type": exec_type}}
    if delivery is not None:
        entry["delivery"] = delivery
    return entry


def enable_stinger(
    node: tt.AnyNode, rules: Iterable[dict[str, Any]], directory: Path, *, match_during_sync: bool = False
) -> Path:
    """
    Configure `node` (not yet running) to load the stinger plugin with `rules` and serve them on a unix socket.

    The rules file and the socket are placed in `directory`, which must be short enough for a unix socket address
    (108 bytes) - the node's own directory usually is not. By default the plugin starts matching when the node enters
    live mode; `match_during_sync` makes it match synced blocks too. Returns the socket path.
    """
    directory.mkdir(parents=True, exist_ok=True)
    rules_file = directory / "stinger_rules.json"
    rules_file.write_text(json.dumps({"rules": list(rules)}, indent=2))
    socket_path = directory / "stinger.sock"

    implementation = get_implementation(node, Node)
    process = implementation._exec  # noqa: SLF001
    current = process.config
    replaced = StingerNodeConfig(
        **msgspec.structs.asdict(current),
        stinger_rules_file=str(rules_file),
        stinger_socket=str(socket_path),
        stinger_match_during_sync=match_during_sync,
    )
    # the executable keeps its config in a name-mangled attribute of beekeepy's Executable
    process._Executable__config = replaced  # type: ignore[attr-defined] # noqa: SLF001
    assert node.config is replaced, "the node did not pick up the replaced config"
    if "stinger" not in node.config.plugin:
        node.config.plugin.append("stinger")
    return socket_path


class StreamReader:
    """
    A consumer of a stinger socket: connects (retrying until the node has created the socket), reads every line on
    a background thread and keeps the parsed records in arrival order.
    """

    def __init__(self, socket_path: Path, *, connect_timeout: float = 60.0) -> None:
        self.socket_path = socket_path
        self._records: list[Record] = []
        self._lock = threading.Lock()
        self._eof = threading.Event()
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        deadline = time.monotonic() + connect_timeout
        while True:
            try:
                self._sock.connect(str(socket_path))
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)
        self._thread = threading.Thread(target=self._read, name=f"stinger-reader-{socket_path.name}", daemon=True)
        self._thread.start()

    def _read(self) -> None:
        with self._sock.makefile("rb") as lines:
            for raw in lines:
                if not raw.endswith(b"\n"):
                    tt.logger.warning("stinger stream: torn last line discarded")
                    break
                record = json.loads(raw)
                with self._lock:
                    self._records.append(record)
        self._eof.set()

    @property
    def records(self) -> list[Record]:
        """A snapshot of everything received so far."""
        with self._lock:
            return list(self._records)

    @property
    def at_eof(self) -> bool:
        return self._eof.is_set()

    def wait_for(
        self, predicate: Callable[[list[Record]], bool], *, timeout: float = 60.0, what: str = ""
    ) -> list[Record]:
        """Block until `predicate` holds on the records received so far; returns them."""
        deadline = time.monotonic() + timeout
        while True:
            records = self.records
            if predicate(records):
                return records
            if self._eof.is_set():
                raise AssertionError(f"stinger stream ended before {what or 'the condition'} held")
            if time.monotonic() > deadline:
                raise TimeoutError(f"stinger stream: {what or 'the condition'} did not hold within {timeout}s")
            time.sleep(0.1)

    def wait_for_eof(self, *, timeout: float = 60.0) -> list[Record]:
        assert self._eof.wait(timeout), "stinger stream did not end"
        return self.records

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()


def matches(records: Iterable[Record], *, delivery: str | None = None, rule_name: str | None = None) -> list[Record]:
    """The match records, optionally of one delivery class and/or one rule."""
    return [
        r
        for r in records
        if r["kind"] == "match"
        and (delivery is None or r["delivery"] == delivery)
        and (rule_name is None or rule_name in r["rules"])
    ]


def markers(records: Iterable[Record], kind: str) -> list[Record]:
    """The block_applied / irreversible / switch_fork records."""
    return [r for r in records if r["kind"] == kind]


def delivered_head(hello: Record) -> int:
    """The head stream's position a hello reports: the last block delivered at head, 0 for none."""
    head = hello["delivered"]["head"]
    return 0 if head is None else head["block"]


def _assert_hello_well_formed(hello: Record) -> None:
    assert hello["kind"] == "hello", f"first record is {hello}"
    assert hello["version"] == 1
    assert isinstance(hello["seq"], int), hello
    assert hello["seq"] >= 0, hello
    delivered = hello["delivered"]
    assert set(delivered) == {"head", "irreversible"}, hello
    head = delivered["head"]
    if head is not None:
        assert set(head) == {"block", "block_id"}, hello
        assert isinstance(head["block"], int), hello
        assert head["block"] >= 1, hello
        assert isinstance(head["block_id"], str), hello
        assert len(head["block_id"]) == 40, hello
    # null while the irreversible stream has not started; 0 once it has and nothing is irreversible yet
    irreversible = delivered["irreversible"]
    assert irreversible is None or (isinstance(irreversible, int) and irreversible >= 0), hello


def _assert_head_position_is_a_cut(hello: Record, sequenced: list[Record]) -> None:
    """Every head record after the hello is above its head position, and above every switch_fork since."""
    floor = delivered_head(hello)
    for r in sequenced:
        if r["kind"] == "switch_fork":
            assert len(r["block_id"]) == 40, r
            floor = r["block"]
        elif r["kind"] == "block_applied" or (r["kind"] == "match" and r["delivery"] == "head"):
            assert r["block"] > floor, f"{r} is at or below the head position {floor}"


def assert_well_formed(records: list[Record]) -> None:
    """The invariants every stream must hold: hello first, then seq contiguous from it, every record with a kind."""
    assert records, "no records at all"
    hello = records[0]
    _assert_hello_well_formed(hello)
    # the hello stands at the last record rendered before the connection; what follows counts up from there
    sequenced = records[1:]
    expected = hello["seq"] + 1
    for r in sequenced:
        assert r["seq"] == expected, f"seq gap: expected {expected}, got {r}"
        expected += 1
    for r in sequenced:
        assert r["kind"] in {"match", "block_applied", "irreversible", "switch_fork"}, r
    # irreversible matches may trail below the hello's irreversible bound; nothing is promised about them
    _assert_head_position_is_a_cut(hello, sequenced)
