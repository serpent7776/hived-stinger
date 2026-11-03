#!/usr/bin/env bash
# Try stinger on a single-node testnet, in one command.
#
# Starts a testnet hived with the witness and stinger plugins and the rules
# in rules.json, attaches consumer.py to its socket, then uses cli_wallet to
# create two accounts and move some TESTS between them so that the rules
# fire. Everything the consumer receives is printed to the terminal; the
# node, wallet and consumer output are kept in the demo's data directory.
#
# Needs a testnet build of this branch (cmake -DBUILD_HIVE_TESTNET=ON) and
# python3. Binaries are taken from build/ at the top of the repo unless
# HIVED_PATH / CLI_WALLET_PATH or the options below say otherwise.

set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../../../.." && pwd)

hived=${HIVED_PATH:-$repo/build/programs/hived/hived}
cli_wallet=${CLI_WALLET_PATH:-$repo/build/programs/cli_wallet/cli_wallet}
duration=120
data_dir=
keep=0
raw=0
p2p_port=3101
api_port=3102

# the testnet's initminer key, the same on every testnet build
initminer_wif=5JNHfZYKGaomSFvd4NUdQ9qMcEAC43kujbfjueTHpVapX1Kzq2n

usage()
{
  cat <<USAGE
usage: $(basename "$0") [options]

  --hived PATH        testnet hived binary (default: \$HIVED_PATH or build/programs/hived/hived)
  --cli-wallet PATH   cli_wallet binary (default: \$CLI_WALLET_PATH or build/programs/cli_wallet/cli_wallet);
                      without one the demo runs, but only the per-block rule fires
  --duration SECONDS  how long to let the node run (default: $duration; irreversible delivery
                      starts moving around block 22, about 70 s in)
  --data-dir DIR      run the node here instead of a temporary directory; never deleted
  --keep              keep the temporary data directory (logs, block_log, consumer output)
  --raw               print the records as JSON lines instead of one readable line each
  --p2p-port N        port the node's p2p plugin listens on (default: $p2p_port; nothing connects to it)
  --api-port N        port for the node's HTTP and websocket API (default: $api_port)
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --hived) hived=$2; shift 2 ;;
    --cli-wallet) cli_wallet=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --data-dir) data_dir=$2; shift 2 ;;
    --keep) keep=1; shift ;;
    --raw) raw=1; shift ;;
    --p2p-port) p2p_port=$2; shift 2 ;;
    --api-port) api_port=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

say() { printf '\n== %s\n' "$*"; }
die() { echo "error: $*" >&2; exit 1; }

[ -x "$hived" ] || die "no hived at $hived (build with -DBUILD_HIVE_TESTNET=ON, or pass --hived)"
command -v python3 >/dev/null || die "python3 is needed for the consumer"
"$hived" --version 2>/dev/null | grep -q '"node_type":"testnet"' \
  || die "$hived is not a testnet build; the demo needs cmake -DBUILD_HIVE_TESTNET=ON"
grep -q -- '--stinger-rules-file' <<<"$("$hived" --help 2>/dev/null)" \
  || die "$hived was built without the stinger plugin"
if [ ! -x "$cli_wallet" ]; then
  echo "note: no cli_wallet at $cli_wallet; skipping the transactions, only initminer-produced will fire" >&2
  cli_wallet=
fi

created_dir=0
if [ -z "$data_dir" ]; then
  data_dir=$(mktemp -d "${TMPDIR:-/tmp}/stinger-demo.XXXXXX")
  created_dir=1
fi
mkdir -p "$data_dir"
data_dir=$(cd "$data_dir" && pwd)

if [ -e "$data_dir/rules.json" ]; then
  echo "using the rules.json already in $data_dir"
else
  cp "$here/rules.json" "$data_dir/rules.json"
fi
cat > "$data_dir/config.ini" <<CONFIG
# written by $(basename "$0")
plugin = witness webserver json_rpc database_api block_api network_broadcast_api rc_api account_by_key account_by_key_api wallet_bridge_api stinger

shared-file-dir = blockchain
shared-file-size = 256M
p2p-endpoint = 127.0.0.1:$p2p_port
webserver-http-endpoint = 127.0.0.1:$api_port
webserver-ws-endpoint = 127.0.0.1:$api_port

# a lone witness producing from genesis
witness = "initminer"
private-key = $initminer_wif
enable-stale-production = 1
required-participation = 0

stinger-rules-file = rules.json
stinger-socket = stinger.sock

log-appender = {"appender":"stderr","stream":"std_error","time_format":"iso_8601_milliseconds"}
log-logger = {"name":"default","level":"info","appender":"stderr"} {"name":"user","level":"info","appender":"stderr"} {"name":"p2p","level":"warn","appender":"stderr"} {"name":"sync","level":"warn","appender":"stderr"}
CONFIG

hived_pid=
consumer_pid=
cleanup()
{
  trap - EXIT INT TERM
  if [ -n "$hived_pid" ] && kill -0 "$hived_pid" 2>/dev/null; then
    say "stopping hived"
    kill -TERM "$hived_pid" 2>/dev/null || true
    wait "$hived_pid" 2>/dev/null || true
  fi
  if [ -n "$consumer_pid" ]; then
    # it exits on the socket's EOF; if the socket never appeared it is still
    # waiting for it
    kill -0 "$consumer_pid" 2>/dev/null && { sleep 1; kill "$consumer_pid" 2>/dev/null || true; }
    wait "$consumer_pid" 2>/dev/null || true
  fi
  if [ -f "$data_dir/hived.log" ]; then
    say "sink shutdown report from hived.log"
    grep -E 'stop_sink' "$data_dir/hived.log" | sed -E 's/^.*\] //' | sed 's/^/   /' || true
  fi
  if [ "$created_dir" = 1 ] && [ "$keep" = 0 ]; then
    rm -rf "$data_dir"
    echo
    echo "temporary data directory removed; rerun with --keep to look at the logs"
  else
    echo
    echo "data directory kept at $data_dir:"
    echo "   hived.log      node output, including the sink's shutdown report"
    echo "   consumer.out   every record the consumer printed"
    [ -n "$cli_wallet" ] && echo "   wallet.log     what cli_wallet did"
    echo "   rules.json     the rules the node ran; edit and rerun with --data-dir $data_dir"
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

say "data directory $data_dir"
say "starting hived (log: $data_dir/hived.log)"
"$hived" -d "$data_dir" > "$data_dir/hived.log" 2>&1 &
hived_pid=$!

consumer_args=(--testnet)
[ "$raw" = 1 ] && consumer_args+=(--raw)
python3 "$here/consumer.py" "$data_dir/stinger.sock" "${consumer_args[@]}" > >(tee "$data_dir/consumer.out") 2>&1 &
consumer_pid=$!

# head block number through the node's HTTP API, empty while it is not up
head_block()
{
  python3 - "$api_port" <<'PY' 2>/dev/null || true
import json, sys, urllib.request
req = urllib.request.Request(
    f"http://127.0.0.1:{sys.argv[1]}",
    data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "database_api.get_dynamic_global_properties", "params": {}}).encode(),
    headers={"Content-Type": "application/json"})
print(json.load(urllib.request.urlopen(req, timeout=2))["result"]["head_block_number"])
PY
}

wait_for_block()
{
  local target=$1 deadline=$((SECONDS + 90)) head
  while :; do
    kill -0 "$hived_pid" 2>/dev/null || die "hived exited; see $data_dir/hived.log"
    head=$(head_block)
    [ -n "$head" ] && [ "$head" -ge "$target" ] && return 0
    [ $SECONDS -lt $deadline ] \
      || die "the node's API on 127.0.0.1:$api_port did not report block $target within 90 s (port in use?); see $data_dir/hived.log"
    sleep 1
  done
}

say "waiting for the node to produce its first blocks (one every 3 s)"
wait_for_block 3

if [ -n "$cli_wallet" ]; then
  say "creating alice and shop, paying them from initminer (wallet log: $data_dir/wallet.log)"
  # cli_wallet reads commands from stdin when it is not a terminal and exits at EOF
  "$cli_wallet" -s "ws://127.0.0.1:$api_port" -w "$data_dir/wallet.json" --retry-server-connection \
    > "$data_dir/wallet.log" 2>&1 <<WALLET || echo "note: cli_wallet exited with an error; see $data_dir/wallet.log" >&2
set_password demo
unlock demo
import_key $initminer_wif
create_account initminer alice "{}" true
create_account initminer shop "{}" true
transfer initminer alice "10.000 TESTS" "welcome" true
transfer_to_vesting initminer alice "5.000 TESTS" true
transfer initminer shop "1250.000 TESTS" "order 42" true
transfer alice shop "2.000 TESTS" "order 43" true
WALLET
  # cli_wallet exits 0 whatever its commands did; a rejected command is an
  # exception line in its log
  failed=$(grep -cE '^[0-9]+ [a-z_]*exception: ' "$data_dir/wallet.log" || true)
  if [ "$failed" != 0 ]; then
    echo "note: $failed wallet command(s) were rejected; see $data_dir/wallet.log" >&2
    echo "      (on a reused data directory the accounts exist and the wallet is already set up: expected)" >&2
  fi
  cat <<EXPECT

   What to look for:
   - shop-paid and large-transfer are delivered at head: both payments to shop
     appear within a block of being sent, the large one with both rule names.
   - alice-activity is delivered irreversibly: alice's account creation, the
     transfer and power-up to her, and her payment to shop show up only once
     their block cannot be undone. On a fresh testnet the irreversible point
     starts moving at block 22 (about 70 s in) and trails head by 21 blocks,
     so expect those records about a minute after the head ones. alice's
     payment to shop is matched by rules of both classes, so it appears twice:
     once at head for shop-paid, once irreversibly for alice-activity.
   - initminer-produced fires once per block, at head, as a heartbeat.
EXPECT
fi

say "letting the node run for $duration s (Ctrl-C stops it early)"
end=$((SECONDS + duration))
while [ $SECONDS -lt $end ]; do
  kill -0 "$hived_pid" 2>/dev/null || die "hived exited; see $data_dir/hived.log"
  sleep 1
done
