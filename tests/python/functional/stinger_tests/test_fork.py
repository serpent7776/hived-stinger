from __future__ import annotations

import json
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

# 21 scheduled witnesses: initminer and 4 more on the node under test, 16 on the other side. 16 is what the
# irreversibility threshold needs, so only the majority's blocks ever become irreversible; the minority keeps
# producing on its own (required participation 0) and has to retract when the networks meet again.
MINORITY_WITNESSES = [f"minority-{i}" for i in range(4)]
MAJORITY_WITNESSES = [f"majority-{i}" for i in range(16)]
ALL_WITNESSES = MINORITY_WITNESSES + MAJORITY_WITNESSES


def op_key(record: dict) -> tuple:
    """What identifies an operation across the two streams: its coordinates and its body."""
    return (record["block"], record["trx_in_block"], record["op_in_trx"], json.dumps(record["op"], sort_keys=True))


@pytest.mark.testnet
def test_head_stream_retracts_a_lost_fork(tmp_path: Path) -> None:
    minority = tt.Network()
    majority = tt.Network()
    node = tt.InitNode(network=minority)
    for name in MINORITY_WITNESSES:
        node.config.witness.append(name)
        node.config.private_key.append(tt.Account(name).private_key)
    other = tt.WitnessNode(witnesses=MAJORITY_WITNESSES, network=majority)
    minority.connect_with(majority)

    socket_path = enable_stinger(
        node,
        [
            rule("head", "op_type >= 0", delivery="head"),
            rule("final", "op_type >= 0"),
        ],
        tmp_path,
    )
    minority.run()
    majority.run()
    reader = StreamReader(socket_path)
    wallet = tt.Wallet(attach_to=node)

    # vote the 20 witnesses in with initminer's stake
    wallet.api.transfer_to_vesting("initminer", "initminer", tt.Asset.Test(10_000))
    for name in ALL_WITNESSES:
        wallet.create_account(name, vests=tt.Asset.Test(10))
    with wallet.in_single_transaction():
        for name in ALL_WITNESSES:
            wallet.api.update_witness(
                name,
                f"https://{name}.example",
                tt.Account(name).public_key,
                {"account_creation_fee": tt.Asset.Test(3), "maximum_block_size": 65536, "hbd_interest_rate": 0},
            )
    with wallet.in_single_transaction():
        for name in ALL_WITNESSES:
            wallet.api.vote_for_witness("initminer", name, True)

    tt.logger.info("waiting for the schedule to include every witness")
    for _ in range(80):
        active = set(node.api.database.get_active_witnesses().witnesses)
        if set(ALL_WITNESSES) <= active:
            break
        node.wait_number_of_blocks(1)
    else:
        pytest.fail(f"witnesses never all became active: {active}")
    # let a full round run with everybody producing, so both sides have blocks of the others
    node.wait_number_of_blocks(21)

    tt.logger.info("splitting the networks")
    split_at = node.get_last_block_number()
    minority.disconnect_from(majority)
    # the majority runs ahead; the minority produces a few blocks of its own in the meantime
    other.wait_number_of_blocks(12)
    minority_head = node.get_last_block_number()
    majority_head = other.get_last_block_number()
    tt.logger.info(f"split at {split_at}: minority head {minority_head}, majority head {majority_head}")
    assert minority_head > split_at, "the minority produced nothing while split"
    assert majority_head > minority_head, "the majority chain is not longer"
    assert node.get_last_irreversible_block_number() <= split_at, "the minority made a block irreversible on its own"

    before_merge = reader.records
    own_blocks = [m for m in markers(before_merge, "block_applied") if m["block"] > split_at]
    assert own_blocks, "the head stream never announced the minority's own blocks"
    own_heights = {m["block"] for m in own_blocks}

    tt.logger.info("reconnecting")
    minority.connect_with(majority)
    # the minority switches to the majority's chain and irreversibility catches up past the split
    for _ in range(60):
        if node.get_last_irreversible_block_number() > majority_head:
            break
        node.wait_number_of_blocks(1)
    else:
        pytest.fail("irreversibility did not pass the fork")

    records = reader.wait_for(
        lambda rs: bool(markers(rs, "switch_fork"))
        and any(m["block"] > majority_head for m in markers(rs, "irreversible")),
        what="the retraction and the irreversible marker past the fork",
    )
    assert_well_formed(records)

    # the retraction: one switch_fork naming the last common height, after the minority's own blocks were announced
    retractions = markers(records, "switch_fork")
    assert len(retractions) == 1, retractions
    retraction = retractions[0]
    assert split_at - 1 <= retraction["block"] < min(own_heights), retraction
    assert all(retraction["seq"] > m["seq"] for m in own_blocks)
    # it names the block at the common height: the one announced there before the retraction
    announced_ids = {
        m["block"]: m["block_id"] for m in markers(records, "block_applied") if m["seq"] < retraction["seq"]
    }
    assert retraction["block_id"] == announced_ids[retraction["block"]], retraction

    # every retracted height was announced again afterwards, as a different block
    announced_after = {m["block"]: m for m in markers(records, "block_applied") if m["seq"] > retraction["seq"]}
    for own in own_blocks:
        again = announced_after.get(own["block"])
        assert again is not None, f"height {own['block']} was never re-announced after the retraction"
        assert again["block_id"] != own["block_id"], f"height {own['block']} re-announced with the same block id"

    # the head stream saw each retracted height twice; the irreversible stream saw it once, with the winning
    # branch's operations - exactly the ones the head stream announced after the retraction
    for height in sorted(own_heights):
        head_before = {
            op_key(m)
            for m in matches(records, delivery="head")
            if m["block"] == height and m["seq"] < retraction["seq"]
        }
        head_after = {
            op_key(m)
            for m in matches(records, delivery="head")
            if m["block"] == height and m["seq"] > retraction["seq"]
        }
        final = [m for m in matches(records, delivery="irreversible") if m["block"] == height]
        final_keys = {op_key(m) for m in final}
        assert head_before, f"no head matches for the minority's block {height}"
        assert head_after, f"no head matches for the winning block {height}"
        assert len(final) == len(final_keys), f"an operation of block {height} was delivered twice as irreversible"
        assert final_keys == head_after, f"irreversible delivery of block {height} differs from the winning branch"
        assert all(
            m["seq"] > retraction["seq"] for m in final
        ), f"block {height} was irreversible before the retraction"
    # no irreversible delivery ever retreats
    final_blocks = [m["block"] for m in matches(records, delivery="irreversible")]
    assert final_blocks == sorted(final_blocks)
