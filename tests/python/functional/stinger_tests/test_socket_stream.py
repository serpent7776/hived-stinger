from __future__ import annotations

from pathlib import Path

import pytest

import test_tools as tt
from hive_local_tools.functional.stinger import (
    StreamReader,
    assert_well_formed,
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
    # the late consumer's hello says where it joined, and from there both saw the same records
    joined_at = second_records[0]["next_seq"]
    assert joined_at > 1
    first_from_join = [r for r in first_records if "seq" in r and r["seq"] >= joined_at]
    second_sequenced = [r for r in second_records if "seq" in r]
    common = min(len(first_from_join), len(second_sequenced))
    assert common > 0
    assert first_from_join[:common] == second_sequenced[:common]
