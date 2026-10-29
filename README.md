# Hive - Decentralizing the exchange of ideas and information

## Stinger: matched operations, pushed to your process

This repo adds `stinger`, a hived plugin that watches every operation the
node applies, real and virtual, and hands the ones that match your rules to
your own process as JSON lines on a unix socket. It exists for the
integration that today means either running HAF or polling the APIs: "tell
me when someone pays my account", "give me every custom_json my app
emits", "alert on transfers over a threshold". A rules file does it in a few
lines (a fuller one is at
[libraries/plugins/stinger/example_rules.json](libraries/plugins/stinger/example_rules.json)):

```json
{ "rules": [
  { "name": "shop-paid",
    "match": "op_type in (transfer, fill_recurrent_transfer) and to = \"shop\"",
    "exec": { "type": "socket" } },
  { "name": "large-hive-transfer",
    "match": "op_type = transfer and symbol = \"HIVE\" and amount > 1000000",
    "exec": { "type": "socket" }, "delivery": "head" }
] }
```

```
plugin = stinger
stinger-rules-file = rules.json
stinger-socket = /run/hived/stinger.sock
```

Why it is shaped the way it is:

* **Matching happens where the data is.** Rules are evaluated on the node's
  writer thread as each operation is applied, against a boolean-expression
  index (a vendored be-tree), so the cost is a bounded microsecond or so per
  operation and no user code ever runs on the consensus path. Rules are
  declarative JSON, parsed at startup; there is no script.
* **Irreversible by default, head when you ask.** Hive makes a block
  irreversible about one block after it is applied, so the default stream
  delivers a block's matches once they cannot be undone and consumers never
  see a fork. A rule can opt into `head` delivery for one block less latency,
  and then gets `block_applied`, `switch_fork` and `irreversible` markers to
  retract and settle with.
* **The node keeps no history and no consumer state.** The stream is
  ephemeral; a consumer owns its own storage and fork policy. Every record is
  self-contained (the full operation in database_api form, block, trx id,
  timestamp), the greeting on connect says where each stream stands, and
  what came before that is fetched from the node's APIs. Nothing here
  rebuilds HAF.
* **The writer never waits.** Matches go to a sink thread through a queue
  bounded in bytes (`stinger-ready-queue-mb`, 128 MB by default); if a
  consumer falls behind the socket it is disconnected, if the sink falls
  behind the writer the oldest blocks' matches are dropped and every
  consumer gets a `dropped` record naming the blocks. The failure is loud and
  the node is unaffected.
* **No outbound network.** The sinks are a log line and a local socket;
  webhooks, queues and WebSockets belong in a bridge process on the far side
  of the socket, not inside hived.

### What a rule can match on

| Attribute | Type | Meaning |
|---|---|---|
| `op_type` | integer | the operation's type, written by name (`transfer`, `custom_json_operation`, `producer_reward`) or by its number in the protocol's operation variant; virtual operations included |
| `is_virtual` | boolean | whether the operation is virtual; tested bare: `is_virtual`, `not is_virtual` |
| `impacted_accounts` | list of strings | every account the operation touches, as account history sees it (sender and receiver, author and voter, the witness of a reward) |
| `amount`, `symbol` | integer, string | the operation's own `amount` field, in satoshis, and its asset symbol (`HIVE`, `HBD`, `VESTS`, or `TESTS` / `TBD` on a testnet); the transfer family, the savings and recurrent transfers and their virtual fills, and the two converts have one. Undefined elsewhere, so `amount > X` alone never fires on a fill_order or a reward |
| `custom_id` | string | the `id` of a custom_json operation; undefined for other types |
| `from`, `to` | string | the operation's own `from` and `to` account fields: transfers, savings, escrows, recurrent transfers and their fills. Undefined when the operation has no such field or leaves it empty (a power-up to oneself has no `to`) |

Attributes compare to literals only, never to each other: `from = to` is
not expressible, and a derived flag for such a relation is added on
request rather than guessed. An undefined attribute fails every test on
it, which is why a rule on `amount` or `custom_id` usually carries an
`op_type` term as well, and why one on `impacted_accounts` alone is the
expensive kind (every operation reaches it; keep those to a few dozen).

### The expression language

Rules are boolean expressions over those attributes, parsed once at
startup, with nothing to execute:

* comparisons `=`, `<>`, `<`, `<=`, `>`, `>=` between an attribute and a
  number or a quoted string, either side first: `amount >= 1000000`,
  `"HIVE" = symbol`;
* membership `in` and `not in` with a list in parentheses: `op_type in
  (transfer, transfer_to_vesting)`, `custom_id not in ("sm_market",
  "sm_sell")`; and for the list attribute, a string against it: `"alice"
  in impacted_accounts`, or a list against it with `one of`, `none of`,
  `all of`: `impacted_accounts one of ("alice", "bob")`;
* `and`, `or`, `not` and parentheses, with `not` binding tightest;
* a bare boolean attribute as a term: `is_virtual`.

Operation names are rewritten to their numbers before parsing, so a name
is valid wherever an integer is. Strings are case-sensitive and compared
whole; there is no pattern matching. A rule that does not parse stops the
node at startup, naming the rule.

Matching starts when the node is live (`stinger-match-during-sync` turns it
on for the sync too). Meant for API and seed nodes; it is discretionary
work on the block-apply path, so not for witness nodes. The record format
and the consumer contract are documented in
[libraries/plugins/stinger/](libraries/plugins/stinger/), starting from
[example_rules.json](libraries/plugins/stinger/example_rules.json) and
[records.hpp](libraries/plugins/stinger/include/hive/plugins/stinger/records.hpp).


[![GitLab](https://img.shields.io/badge/Development%20on-Community%20GitLab-orange)](https://gitlab.syncad.com/hive/hive)

This project is primarily developed on a community-hosted GitLab instance. Please visit the [community GitLab repository](https://gitlab.syncad.com/hive/hive) for the latest updates, issues, and contributions.

![Hive](https://files.peakd.com/file/peakd-hive/netuoso/jMHldwMV-horizontal.png)

Hive is a Graphene-based social blockchain that was created as a fork of Steem and born on the core idea of decentralization. Originally, Hive was announced on the [Steem blockchain](https://peakd.com/communityfork/@hiveio/announcing-the-launch-of-hive-blockchain) prior to the initial token airdrop. Hive did not have any ICO or mining period.

The Hive blockchain eliminates the elements of centralization and imbalanced control that plagued the Steem blockchain. Since its launch on March 20, 2020, Hive has been continuously growing and evolving. Hive's prime selling points are its decentralization, 3-second transaction speed, and ability to handle large volumes. It is an ideal platform for a variety of innovative projects focused on a broad range of fields, from open source development to games.

Hive serves as the operational home for all kinds of projects, companies, and applications. With a highly active and passionate community, Hive has become a thriving environment for both new and experienced developers to quickly bootstrap their applications. On top of this, Hive is extremely rewarding to content creators and curators alike.

The technical development of the Hive blockchain is carried out by a founding decentralized group of over 30 open source developers, many of whom were instrumental in creating Steem back in 2016. This core team is supported by a growing community of additional open source developers and witnesses.

## Documents

* Developer Portal: https://developers.hive.io/

## Advantages

* Hive Fund
* Truly Decentralized Community
* Free Transactions (Resource Credits = Freemium Model)
* Fast Block Confirmations (3 seconds)
* One Block Irreversibility
* Time Delay Security (Vested Hive & Savings)
* Hierarchical Role Based Permissions (Keys)
* Integrated Token Allocation
* Lowest Entry-Barrier for User Adoption in the market
* Dozens of dApps already built on Hive and many more to come

## Technical Details

* Currency symbol HIVE
* HBD - Hive's very own stable coin with a two-way peg
* Delegated Proof-of-Stake Consensus (DPoS)
* 10% APR inflation narrowing to 1% APR over 20 years
    * 65% of inflation to authors/curators.
    * 15% of inflation to stakeholders.
    * 10% of inflation to block producers.
    * 10% of inflation to Hive Fund.

# Installation

Getting started with Hive is fairly simple. You can either choose to use pre-built Docker images, build with Docker manually, or build from source directly. All steps have been documented and while many different OSes are supported, the easiest one is Ubuntu 24.04 LTS.

## Quickstart

Want to get up and running quickly? We offer pre-built Docker images for your convenience. More details can be found in our [Quickstart Guide](doc/exchangequickstart.md).

## Building

We **strongly** recommend using one of the pre-built Docker images or building Hive using Docker. Both processes are described in the [Quickstart Guide](doc/exchangequickstart.md).

However, if you prefer to build from source, we also provide [build instructions](doc/building.md) for Linux (Ubuntu LTS).

## Dockerized deployment

Building a `hived` docker image is described in detail here: [Building under Docker](doc/building.md#building-under-docker).

If you'd like to use our pre-built official binary images, download them from the Docker Hub registry with the specific version tag. For example, to use version 1.28.0, run:

```
docker pull hiveio/hive:1.28.0
```

Please note that the `latest` tag is not supported; you must explicitly specify the version tag to avoid issues with new versions that require replay.

The recommended way to run the image is the Docker Compose deployment in [docker/exchange](docker/exchange): a `hived` service with account history, an optional `cli_wallet` daemon, a health check, and a data volume that bootstraps itself on the first start from a published state snapshot or a block log. It is configured entirely through `compose.yml`, `.env` and `config.ini`; see [docker/exchange/README.md](docker/exchange/README.md).

The image can also be started by hand. [run_hived_img.sh](scripts/run_hived_img.sh) wraps `docker run` while emulating direct `hived` usage: `run_hived_img.sh <docker_img> [OPTION[=VALUE]]... [<hived_option>]...`; read [more about using run_hived_img.sh](doc/run_hived_img.md) to explore its usage in various scenarios.

Environment variables understood by the image's entrypoint:

- `BLOCK_LOG_URL`: URL of a monolithic `block_log` to download on the first start when the data directory holds no block log yet (its `.artifacts` sidecar is fetched from `BLOCK_LOG_ARTIFACTS_URL`, default `BLOCK_LOG_URL.artifacts`).
- `SNAPSHOT_URL`: URL of a `.tgz` state snapshot (as produced by `--dump-snapshot`) to download and load on the first start instead of replaying from genesis. Without a block log it implies `block-log-split=0` (no block log is kept); with `BLOCK_LOG_URL` the block log is kept and replayed forward from the snapshot. hived writes its RocksDB files (snapshots, comment archive, account history) with RocksDB table format_version 7, which a hived built with RocksDB older than 10.11 (hive's RocksDB 9.10 builds) cannot read, so a snapshot dumped by a current build does not load in such an older build; older snapshots and databases (format_version 6) still load in a current one.
- `HIVED_AUTO_REPLAY`: set to `1` to have the entrypoint add `--replay-blockchain` whenever a block log is present and no replay mode was requested, so a downloaded or copied-in block log is replayed without further action. Off by default, so a mismatch between the state and the block log stops hived with an error instead of starting a replay. The exchange compose deployment turns it on.
- `HIVED_UID`: run hived as this UID (non-zero, numeric), for bind-mounted directories owned by a user other than 1000, typically `-e HIVED_UID=$(id -u)`; see [run_hived_img.md](doc/run_hived_img.md) for details.
- `P2P_ENDPOINT`, `WS_ENDPOINT`, `HTTP_ENDPOINT`: listening endpoints inside the container.

`/home/hived/scripts/hived_healthcheck.sh` is a health check for the container: it exits 0 once hived answers JSON-RPC and its head block is at most `HIVED_HEALTHCHECK_MAX_BLOCK_AGE` seconds old (default 120).

## CLI Wallet

We provide a basic cli wallet for interfacing with `hived`. The wallet is self-documented via command line help. The node you connect to via the cli wallet needs to be running the `account_by_key_api`, `condenser_api`, `database_api`, `account_history_api`, `wallet_bridge_api` plugins and needs to be configured to accept WebSocket connections via `webserver-ws-endpoint`.

The cli_wallet offers two operating modes:
- interactive mode: commands are entered on an interactive command line
- daemon mode: wallet commands are sent via RPC calls. In daemon mode it is important to specify which IP addresses are allowed to establish connections to the wallet's HTTP server. See `--rpc-http-endpoint`, `--daemon`, `--rpc-http-allowip` options for details.

To prepare transactions, you need to execute a few setup steps in the cli_wallet:
- use set_password <password> call to establish a password protecting your wallet from unauthorized access
- use unlock <password> to turn on full-operation mode in the wallet
- import private key(s) using import_key "WIF-key" command

Keys and password are stored in the wallet.json file.

## Testing

See [doc/devs/testing.md](doc/devs/testing.md) for testing build targets and info
on how to use lcov to check code coverage of tests.

## Changes spanning hive, wax and hive-local-tools

The Python tests of hive use `tests/python/hive-local-tools`, which pins packages released by other repositories:
`hiveio-wax` and `hiveio-test-tools` (built by [wax](https://gitlab.syncad.com/hive/wax) from its `hive` submodule)
and `hiveio-beekeepy` ([beekeeper](https://gitlab.syncad.com/hive/beekeeper)). A change crossing these repositories
goes through separate merge requests, each merged before the next one uses it, so that `develop` never pins
development builds (`<version>.dev<timestamp>`) of another repository:

1. **hive** - the changes wax needs (skipped when wax only updates its `hive` submodule). After the merge the `develop`
   pipeline publishes the release of `hiveio-api` (`<version>.post<timestamp>`).
2. **wax** - points the `hive` submodule and the ref of the hive CI include at that `develop` commit and pins the
   releases of `hiveio-api` and `hiveio-beekeepy`. After the merge the `develop` pipeline of wax publishes the releases
   of `hiveio-wax` and `hiveio-test-tools`.
3. **hive** again - bumps `hive-local-tools` to the `develop` releases of wax and test-tools (and of beekeepy if it
   changed).

The merge request of step 1 is tested with the pins of `develop`, i.e. with the current releases of wax and test-tools:
it must not break them, and tests needing the new wax come with step 3. Development builds may be pinned on a branch to
try the whole chain out, but such a pin is never merged.

# Configuration

## Config File

Run `hived --dump-config` to generate a data directory and a config file. The default data directory location is `~/.hived`. If you want to modify the config to your liking, we have [example config](contrib/config-for-docker.ini) used in the docker image. All options will be present in the default config file and there may be more options that need to be changed from the docker configs (some of the options actually used in images are configured via command line).

## Seed Nodes

A list of some seed nodes to get you started can be found in
[doc/seednodes.txt](doc/seednodes.txt).

This same file is baked into the binary and can be overridden by `--p2p-seed-node`
command option set to a whitespace-delimited list of seed nodes (with port).

## System Requirements

Running a Hive consensus node requires the following hardware resources:
- data directory to hold a blockchain file(s) (~600GB of disk storage is required)
- storage to hold a shared memory file (~12GB of memory is required at the moment to store state data)

# No Support & No Warranty

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
