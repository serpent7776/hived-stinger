from __future__ import annotations

from pathlib import Path

import pytest

import test_tools as tt
from hive_local_tools.functional.stinger import (
    Record,
    StreamReader,
    assert_well_formed,
    delivered_head,
    enable_stinger,
    markers,
    matches,
    rule,
)

TRANSFER_AMOUNT = tt.Asset.Test(100)


@pytest.mark.testnet
def test_matches_reach_a_socket_consumer(tmp_path: Path) -> None:
    """
    Real transactions through the whole path: a rule per attribute kind, a consumer on the socket, and the records
    it gets for a transfer and a custom_json, in both delivery classes.
    """
    node = tt.InitNode()
    socket_path = enable_stinger(
        node,
        [
            # irreversible, the default: satoshis of the primary asset and its symbol
            rule("big-transfer", 'op_type = 2 and symbol = "TESTS" and amount >= 100000'),
            # head: custom_json by id
            rule("follow-json", 'op_type = 18 and custom_id = "follow"', delivery="head"),
            # head: anything touching alice, so the same operations show up under two rules
            rule("alice-head", '"alice" in impacted_accounts', delivery="head"),
            # a log rule must never appear on the socket
            rule("everything-log", "op_type >= 0", exec_type="log"),
        ],
        tmp_path,
    )
    node.run()
    reader = StreamReader(socket_path)
    wallet = tt.Wallet(attach_to=node)

    wallet.create_account("alice", hives=tt.Asset.Test(10), vests=tt.Asset.Test(100))
    transfer = wallet.api.transfer("initminer", "alice", TRANSFER_AMOUNT, "stinger")
    follow = wallet.api.follow("alice", "initminer", ["blog"])

    # head delivery: the custom_json, as soon as its block is applied
    records = reader.wait_for(
        lambda rs: bool(matches(rs, delivery="head", rule_name="follow-json")), what="the follow at head"
    )
    head_follow = matches(records, delivery="head", rule_name="follow-json")[0]
    assert head_follow["op"]["type"] == "custom_json_operation"
    assert head_follow["op"]["value"]["id"] == "follow"
    assert head_follow["op"]["value"]["required_posting_auths"] == ["alice"]
    assert head_follow["trx_id"] == follow.transaction_id
    assert head_follow["virtual_op"] is False
    # alice is impacted by it too, and both head rules are named on the one record
    assert sorted(head_follow["rules"]) == ["alice-head", "follow-json"]

    # the transfer at head is alice's business only; big-transfer is an irreversible rule
    head_transfer = next(
        m for m in matches(records, delivery="head", rule_name="alice-head") if m["trx_id"] == transfer.transaction_id
    )
    assert head_transfer["rules"] == ["alice-head"]
    assert head_transfer["op"]["type"] == "transfer_operation"
    assert head_transfer["op"]["value"]["from"] == "initminer"
    assert head_transfer["op"]["value"]["to"] == "alice"
    assert head_transfer["op"]["value"]["memo"] == "stinger"
    # database_api form: the amount is an nai object, not a legacy string
    assert head_transfer["op"]["value"]["amount"] == {"amount": "100000", "precision": 3, "nai": "@@000000021"}
    assert head_transfer["block"] == transfer.block_num

    # the block's matches precede its block_applied, which carries the block id
    applied = [m for m in markers(records, "block_applied") if m["block"] == transfer.block_num]
    assert applied, "no block_applied for the transfer's block yet"
    assert applied[0]["seq"] > head_transfer["seq"]
    assert len(applied[0]["block_id"]) == 40

    # irreversible delivery: the same transfer, under the irreversible rule, once its block is irreversible
    records = reader.wait_for(
        lambda rs: bool(matches(rs, delivery="irreversible", rule_name="big-transfer")),
        what="the transfer at irreversibility",
    )
    irreversible_transfer = matches(records, delivery="irreversible", rule_name="big-transfer")[0]
    assert irreversible_transfer["rules"] == ["big-transfer"]
    assert irreversible_transfer["trx_id"] == transfer.transaction_id
    assert irreversible_transfer["block"] == transfer.block_num
    assert irreversible_transfer["trx_in_block"] == head_transfer["trx_in_block"]
    assert irreversible_transfer["op_in_trx"] == head_transfer["op_in_trx"]
    assert irreversible_transfer["op"] == head_transfer["op"]
    assert irreversible_transfer["seq"] > head_transfer["seq"]
    # and the head stream was told the height is final by then, or will be shortly
    reader.wait_for(
        lambda rs: any(m["block"] >= transfer.block_num for m in markers(rs, "irreversible")),
        what="the irreversible marker",
    )

    # the 10 TESTS from create_account are under the big-transfer threshold; the 100 TESTS are the only hit
    assert len(matches(records, rule_name="big-transfer")) == 1
    # nothing a log rule matched leaks onto the socket
    assert all("everything-log" not in m["rules"] for m in matches(records))
    # every socket record names only socket rules of its own class
    for m in matches(records, delivery="irreversible"):
        assert set(m["rules"]) <= {"big-transfer"}, m
    for m in matches(records, delivery="head"):
        assert set(m["rules"]) <= {"follow-json", "alice-head"}, m
    assert_well_formed(records)

    # the node's shutdown ends the stream cleanly; the consumer sees EOF after the last complete line
    node.close()
    final = reader.wait_for_eof()
    assert_well_formed(final)
    assert not socket_path.exists(), "the socket file was not removed at shutdown"


@pytest.mark.testnet
def test_a_second_consumer_gets_the_same_stream(tmp_path: Path) -> None:
    node = tt.InitNode()
    socket_path = enable_stinger(node, [rule("everything", "op_type >= 0", delivery="head")], tmp_path)
    node.run()
    first = StreamReader(socket_path)
    node.wait_number_of_blocks(2)
    second = StreamReader(socket_path)
    node.wait_number_of_blocks(3)

    first_records = first.records
    second_records = second.records
    assert_well_formed(first_records)
    assert_well_formed(second_records)
    # the late consumer's hello says where it joined: the first record it gets is the one above the hello's seq, and
    # from there both saw the same records
    joined_at = second_records[0]["seq"] + 1
    assert joined_at > 1
    first_from_join = [r for r in first_records[1:] if r["seq"] >= joined_at]
    second_sequenced = second_records[1:]
    common = min(len(first_from_join), len(second_sequenced))
    assert common > 0
    assert first_from_join[:common] == second_sequenced[:common]

    # its hello names the head stream's position: the block the first consumer saw applied last before the join,
    # and the next block_applied the late consumer gets is the one above it. The first consumer's reader thread may
    # not have caught up to the join yet
    head = second_records[0]["delivered"]["head"]
    assert head is not None, "two blocks in and no head position"
    first_records = first.wait_for(
        lambda rs: any(r["seq"] == joined_at - 1 for r in rs[1:]), what="the first consumer's record before the join"
    )
    applied_before_join = [m for m in markers(first_records, "block_applied") if m["seq"] < joined_at]
    assert applied_before_join, "the first consumer saw no block before the second joined"
    assert head == {"block": applied_before_join[-1]["block"], "block_id": applied_before_join[-1]["block_id"]}
    second_applied = markers(second_records, "block_applied")
    assert second_applied, "the late consumer saw no block_applied"
    assert second_applied[0]["block"] == head["block"] + 1
    # and the irreversible stream's position is a height the chain had made irreversible by then, or one below the
    # first matched block if that is higher; the stream has started, so it is not null
    irreversible = second_records[0]["delivered"]["irreversible"]
    assert irreversible is not None, "two blocks in and the irreversible stream has not started"
    assert 0 <= irreversible <= max(node.get_last_irreversible_block_number(), head["block"]), second_records[0]


# the chain plugin calls a node live once its head block is within a minute of now; a joiner has to sync at least
# this many 3-second blocks to have been "syncing" in that sense
BLOCKS_OLDER_THAN_LIVE_SYNC_GAP = 25


def _join_an_old_chain(tmp_path: Path, *, match_during_sync: bool) -> tuple[tt.ApiNode, StreamReader, int, int, Record]:
    """
    A producer makes a chain older than the live-sync gap, then a stinger node joins and syncs it. Returns the joiner,
    a consumer connected before the joiner went live, the producer's head once the joiner was live, the head position
    that consumer's hello reported (0 if it was there before any block was delivered), and the hello a consumer
    connecting after the joiner is live gets.
    """
    producer = tt.InitNode()
    joiner = tt.ApiNode()
    socket_path = enable_stinger(
        joiner, [rule("everything", "op_type >= 0", delivery="head")], tmp_path, match_during_sync=match_during_sync
    )
    producer.run()
    producer.wait_for_block_with_number(BLOCKS_OLDER_THAN_LIVE_SYNC_GAP)

    joiner.config.p2p_seed_node.append(producer.p2p_endpoint)
    joiner.run(wait_for_live=False)
    early = StreamReader(socket_path)  # the socket exists from plugin initialization, before any block
    joiner.wait_for_live_mode(timeout=120)
    # the joiner went live after a sync batch ending at or below this height
    head_when_live = producer.get_last_block_number()
    joiner.wait_number_of_blocks(2)
    records = early.wait_for(lambda rs: len(markers(rs, "block_applied")) >= 2, what="two blocks at head")
    early_head = delivered_head(records[0])

    # a consumer connecting now is told where the head stream stands: a block the early one has seen applied, and
    # the next block it gets is the one above
    late = StreamReader(socket_path)
    late_records = late.wait_for(lambda rs: bool(markers(rs, "block_applied")), what="the late consumer's first block")
    late.close()
    assert_well_formed(late_records)
    late_head = late_records[0]["delivered"]["head"]
    assert late_head is not None, "live for two blocks and no head position"
    # delivered to the early consumer before the late one was greeted; its reader thread may still be behind
    early_records = early.wait_for(
        lambda rs: any(m["block"] == late_head["block"] for m in markers(rs, "block_applied")),
        what="the early consumer's copy of the late hello's head block",
    )
    seen_by_early = {m["block"]: m["block_id"] for m in markers(early_records, "block_applied")}
    assert seen_by_early[late_head["block"]] == late_head["block_id"], (late_head, seen_by_early)
    assert markers(late_records, "block_applied")[0]["block"] == late_head["block"] + 1
    late_hello = late_records[0]
    assert late_hello["delivered"]["irreversible"] is not None, "live for two blocks and nothing matched"
    return joiner, early, head_when_live, early_head, late_hello


def _applied_blocks_are_contiguous_from(records: list[dict], first: int) -> None:
    applied = [m["block"] for m in markers(records, "block_applied")]
    assert applied, "no block_applied markers"
    assert applied == list(range(applied[0], applied[0] + len(applied))), f"block_applied not contiguous: {applied}"
    assert applied[0] == first, f"first block_applied {applied[0]}, expected {first}"


@pytest.mark.testnet
def test_matching_starts_when_the_node_is_live(tmp_path: Path) -> None:
    """
    By default a joining node syncs the old blocks with matching off and arms the matcher when it goes live: every
    block from there on is reported, nothing below it ever is.
    """
    joiner, early, head_when_live, early_head, late_hello = _join_an_old_chain(tmp_path, match_during_sync=False)
    records = early.wait_for(lambda rs: len(markers(rs, "block_applied")) >= 2, what="two blocks at head")
    assert_well_formed(records)
    # the consumer connected while the joiner was syncing with matching off, so nothing had been delivered: it saw
    # the first matched block itself
    assert early_head == 0, f"blocks were delivered before the joiner was live, up to {early_head}"
    matched_from = markers(records, "block_applied")[0]["block"]
    # the gate held through the blocks older than the gap, and opened with the first block after the sync batch that
    # made the joiner live - not some later one
    assert matched_from > 1, "the synced blocks were matched"
    assert matched_from <= head_when_live + 1, f"matching only started at {matched_from}, live by {head_when_live}"
    _applied_blocks_are_contiguous_from(records, matched_from)
    assert all(m["block"] >= matched_from for m in matches(records))
    # the irreversible stream's position covers the synced blocks the node never matched: a consumer backfills up to
    # it, and the stream carries everything above. The early consumer's hello may already say so: the lowest matched
    # block is stored before the block's batch is pushed, and the sink greets before it delivers
    bound = max(joiner.get_last_irreversible_block_number(), matched_from - 1)
    assert matched_from - 1 <= late_hello["delivered"]["irreversible"] <= bound, late_hello
    early_irreversible = records[0]["delivered"]["irreversible"]
    assert early_irreversible is None or matched_from - 1 <= early_irreversible <= bound, records[0]
    joiner.close()
    assert_well_formed(early.wait_for_eof())


@pytest.mark.testnet
def test_match_during_sync_backfills_the_synced_blocks(tmp_path: Path) -> None:
    """With stinger-match-during-sync the synced blocks are matched too: the stream starts at block 1."""
    joiner, early, head_when_live, early_head, late_hello = _join_an_old_chain(tmp_path, match_during_sync=True)
    records = early.wait_for(lambda rs: len(markers(rs, "block_applied")) >= 2, what="two blocks at head")
    assert_well_formed(records)
    # every block was matched, so the irreversible stream's position is the chain's own: nothing to skip below it
    assert 0 <= late_hello["delivered"]["irreversible"] <= joiner.get_last_irreversible_block_number(), late_hello
    # the joiner syncs fast, so the consumer may have missed delivered blocks; the hello says how many, and the stream
    # takes over exactly there
    _applied_blocks_are_contiguous_from(records, early_head + 1)
    if early_head == 0:
        assert any(m["block"] == 1 for m in matches(records)), "block 1 has virtual operations and none matched"
    else:
        assert early_head < head_when_live, f"the blocks delivered before the consumer connected reach {early_head}"
    joiner.close()
    assert_well_formed(early.wait_for_eof())
