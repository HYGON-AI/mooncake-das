#!/usr/bin/env bash
# mc4.sh - run one role of the Mooncake key-scale test on THIS node.
#
#   ./mc4.sh preflight             # check this node before starting anything
#   ./mc4.sh master                # start mooncake_master (master role)
#   ./mc4.sh segment holder1       # start one holder segment
#   ./mc4.sh probe                 # start the latency probe (no segment)
#   ./mc4.sh writer                # start the small-key writer (owns one segment)
#   ./mc4.sh wait-evict [timeout]  # wait for [EVICT-TRIGGER] in master.log
#   ./mc4.sh restart-holder holder1
#   ./mc4.sh status
#   ./mc4.sh stack                 # capture master thread stacks with gdb
#   ./mc4.sh collect               # tar logs + metrics into the run dir
#   ./mc4.sh stop [--keep-master]
#
# Invoked by driver/ks_control.py through docker exec; CONF and MC4_ENV_FILE are pushed per run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="$(cd "$ROOT/.." && pwd)"
CONF="${CONF:-$ROOT/nodes.conf}"
WORKSPACE="${WORKSPACE:-/workspace}"

# ks_control.py pushes the per-run knobs here before starting anything.
if [[ -f "${MC4_ENV_FILE:-$ROOT/run.env}" ]]; then
  # shellcheck disable=SC1090
  source "${MC4_ENV_FILE:-$ROOT/run.env}"
fi

PROFILE="${PROFILE:-product}"
if [[ "$PROFILE" == "deterministic" ]]; then
  DEFAULT_RPC_THREADS=1
  DEFAULT_HANDLER_THREADS=1
else
  DEFAULT_RPC_THREADS=16
  DEFAULT_HANDLER_THREADS=16
fi

MASTER_PORT="${MASTER_PORT:-50551}"
METADATA_PORT="${METADATA_PORT:-38080}"
METRICS_PORT="${METRICS_PORT:-39003}"
RPC_THREAD_NUM="${RPC_THREAD_NUM:-$DEFAULT_RPC_THREADS}"
HANDLER_WORKER_THREAD_NUM="${HANDLER_WORKER_THREAD_NUM:-$DEFAULT_HANDLER_THREADS}"
CLIENT_TTL="${CLIENT_TTL:-120}"
LEASE="${LEASE:-1h}"
MINLOGLEVEL="${MINLOGLEVEL:-0}"

# 64GiB -> 67,108,864 keys per segment (offset allocator cap: bytes/1024, 64Mi).
SEGMENT_SIZE="${SEGMENT_SIZE:-64GiB}"
VALUE_SIZE="${VALUE_SIZE:-64}"
BATCH_SIZE="${BATCH_SIZE:-12288}"
WORKERS="${WORKERS:-128}"
MAX_KEYS="${MAX_KEYS:-0}"
KEY_PREFIX="${KEY_PREFIX:-mc4}"
PROBE_INTERVAL="${PROBE_INTERVAL:-1}"
PROBE_WARN_MS="${PROBE_WARN_MS:-1000}"
STOP_GRACE="${STOP_GRACE:-10}"
HOLDER_CLOSE_TIMEOUT="${HOLDER_CLOSE_TIMEOUT:-10}"

# Which mooncake package the run uses:
#   "-"     the package installed in this environment (site-packages) -- default
#   <dir>   a staged tree (put <dir> first on PYTHONPATH, master from <dir>/mooncake/)
RUNTIME_DIR="${RUNTIME_DIR:--}"
LOG_DIR="${LOG_DIR:-/tmp/mc4-$PROFILE}"
MASTER_BIN="${MASTER_BIN:-}"
LOADER="${LOADER:-$ROOT/loader.py}"
HOLDER="${HOLDER:-$ROOT/holder.py}"
PROBE="${PROBE:-$ROOT/probe_exist.py}"

MASTER_HOST="127.0.0.1"
MASTER_SERVER="127.0.0.1:$MASTER_PORT"
METADATA_SERVER="http://127.0.0.1:$METADATA_PORT/metadata"
METRICS_URL="http://127.0.0.1:$METRICS_PORT/metrics"


# Per-role segment size, in order of precedence:
#   1. SEGMENT_SIZE_<ROLE> environment variable (ad-hoc override)
#   2. column 6 of the conf row for that role
#   3. the global SEGMENT_SIZE
SEGMENT_SIZE_OF_ROLE() {
  local role="$1" var i
  var="SEGMENT_SIZE_$(echo "$role" | tr '[:lower:]-' '[:upper:]_')"
  if [[ -n "${!var:-}" ]]; then echo "${!var}"; return; fi
  for i in "${!ROLE[@]}"; do
    if [[ "${ROLE[$i]}" == "$role" && -n "${SEGMENT_SIZES[$i]:-}" ]]; then
      echo "${SEGMENT_SIZES[$i]}"; return
    fi
  done
  echo "$SEGMENT_SIZE"
}

ROLE=(); SSH_HOST=(); LOCAL_HOSTNAME=(); DEVICES=(); SEGMENT_SIZES=()

die() { echo "ERROR: $*" >&2; exit 1; }
note() { echo "[mc4] $*"; }

load_conf() {
  [[ -f "$CONF" ]] || die "missing $CONF; cp nodes.conf.example nodes.conf and edit it"
  local r h c lh d sz extra
  while read -r r h c lh d sz extra; do
    [[ -z "${r:-}" || "$r" == \#* ]] && continue
    [[ -n "${extra:-}" ]] && die "nodes.conf: role=$r has too many columns"
    # 5th column is the segment size when it looks like one, else a device list
    if [[ -n "${d:-}" && -z "${sz:-}" && "$d" =~ ^[0-9]+(\.[0-9]+)?[A-Za-z]*$ ]]; then
      sz="$d"; d=""
    fi
    [[ "${d:-}" == "-" ]] && d=""
    [[ "${sz:-}" == "-" ]] && sz=""
    ROLE+=("$r"); SSH_HOST+=("$h"); LOCAL_HOSTNAME+=("$lh")
    DEVICES+=("${d:-}"); SEGMENT_SIZES+=("${sz:-}")
  done < <(awk 'NF >= 4 && $1 !~ /^#/' "$CONF")
  ((${#ROLE[@]} > 0)) || die "nodes.conf has no entries"

  local i
  for i in "${!ROLE[@]}"; do
    if [[ "${ROLE[$i]}" == "master" ]]; then
      MASTER_HOST="${LOCAL_HOSTNAME[$i]}"
      MASTER_SERVER="$MASTER_HOST:$MASTER_PORT"
      METADATA_SERVER="http://$MASTER_HOST:$METADATA_PORT/metadata"
      METRICS_URL="http://$MASTER_HOST:$METRICS_PORT/metrics"
      break
    fi
  done
}

role_index() {
  local want="$1" i
  for i in "${!ROLE[@]}"; do
    [[ "${ROLE[$i]}" == "$want" ]] && { echo "$i"; return 0; }
  done
  return 1
}

holder_roles() {
  local i
  for i in "${!ROLE[@]}"; do
    case "${ROLE[$i]}" in
      master|writer) ;;
      *) echo "${ROLE[$i]}" ;;
    esac
  done
}

master_bin() {
  local d
  if [[ -n "$MASTER_BIN" ]]; then
    [[ -x "$MASTER_BIN" ]] || die "MASTER_BIN is not executable: $MASTER_BIN"
    echo "$MASTER_BIN"
  elif [[ "$RUNTIME_DIR" != "-" && -x "$RUNTIME_DIR/mooncake/mooncake_master" ]]; then
    echo "$RUNTIME_DIR/mooncake/mooncake_master"
  else
    d="$(python3 -c 'import os, mooncake; print(os.path.dirname(os.path.abspath(mooncake.__file__)))' 2>/dev/null)"
    if [[ -n "$d" && -x "$d/mooncake_master" ]]; then
      echo "$d/mooncake_master"
    elif command -v mooncake_master >/dev/null 2>&1; then
      command -v mooncake_master
    else
      die "PREREQ MISSING: no mooncake_master (install the mooncake package here, or set RUNTIME_DIR/MASTER_BIN); this skill does not build or stage packages"
    fi
  fi
}

common_env() {
  export GLOG_minloglevel="${GLOG_minloglevel:-2}"
  export PROFILE CONF LOG_DIR RUNTIME_DIR MASTER_PORT METADATA_PORT METRICS_PORT
  export PYTHONUNBUFFERED=1
  if [[ "$RUNTIME_DIR" != "-" ]]; then
    export PYTHONPATH="$RUNTIME_DIR${PYTHONPATH:+:$PYTHONPATH}"
  fi
  export LD_LIBRARY_PATH="/opt/dtk/cuda/cuda-12/lib64:/opt/dtk/hip/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export MC_IB_PCI_RELAXED_ORDERING="${MC_IB_PCI_RELAXED_ORDERING:-0}"
  export MC_IB_GID_INDEX="${MC_IB_GID_INDEX:-0}"
  if [[ -n "${MC_EVICT_HOLD_MS:-}" ]]; then
    export MC_EVICT_HOLD_MS
  fi
  unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy 2>/dev/null || true
}

pidfile() { echo "$LOG_DIR/$1.pid"; }

is_running() {
  local f; f="$(pidfile "$1")"
  [[ -f "$f" ]] && kill -0 "$(cat "$f")" 2>/dev/null
}

stop_pidfile() {
  local name="$1" grace="${2:-$STOP_GRACE}" f pid
  f="$(pidfile "$name")"
  [[ -f "$f" ]] || return 0
  pid="$(cat "$f")"
  if kill -0 "$pid" 2>/dev/null; then
    note "stopping $name pid=$pid"
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 $((grace * 10))); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
      note "$name did not exit in ${grace}s, sending SIGKILL"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$f"
}

port_free() {
  python3 - "$1" "$2" <<'PY'
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind((host, port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

wait_port() {
  python3 - "$MASTER_HOST" "$MASTER_PORT" "$(pidfile master)" <<'PY'
import socket, sys, time
host, port, pid_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
pid = open(pid_path).read().strip()
for _ in range(120):
    try:
        with open(f"/proc/{pid}/stat") as handle:
            if handle.read().split()[2] == "Z":
                sys.exit(2)
    except FileNotFoundError:
        sys.exit(2)
    try:
        sock = socket.create_connection((host, port), 1)
        sock.close()
        break
    except OSError:
        time.sleep(0.5)
else:
    sys.exit(3)
print(f"master accepting {host}:{port} pid={pid}")
PY
}

metrics_dump() {
  python3 - "$METRICS_URL" "$METRICS_PORT" <<'PY' || true
import sys, urllib.request
urls = [sys.argv[1], f"http://127.0.0.1:{sys.argv[2]}/metrics"]
text = None
errors = []
for url in urls:
    try:
        text = urllib.request.urlopen(url, timeout=5).read().decode()
        break
    except Exception as exc:
        errors.append(f"{url}: {exc}")
if text is None:
    print("metrics unavailable: " + "; ".join(errors))
    raise SystemExit(0)
wanted = (
    "master_key_count ", "master_active_clients ",
    "master_successful_evictions_total ", "master_evicted_key_count ",
    "master_client_offboarding_queue_depth ",
    "master_client_offboarding_duration_ms ",
    "master_ping_requests_total ",
)
for line in text.splitlines():
    if line.startswith(wanted):
        print(line)
PY
}

start_master() {
  common_env
  mkdir -p "$LOG_DIR"
  local bin; bin="$(master_bin)"
  if is_running master; then
    note "master already running pid=$(cat "$(pidfile master)")"
    return 0
  fi
  port_free "$MASTER_HOST" "$MASTER_PORT" ||
    die "port $MASTER_HOST:$MASTER_PORT is busy; stop the old master or change MASTER_PORT"
  note "master=$bin rpc=$RPC_THREAD_NUM handler=$HANDLER_WORKER_THREAD_NUM profile=$PROFILE"
  "$bin" \
    --rpc_address="$MASTER_HOST" \
    --rpc_port="$MASTER_PORT" \
    --rpc_thread_num="$RPC_THREAD_NUM" \
    --handler_worker_thread_num="$HANDLER_WORKER_THREAD_NUM" \
    --client_ttl="$CLIENT_TTL" \
    --default_kv_lease_ttl="$LEASE" \
    --eviction_high_watermark_ratio="${EVICTION_HIGH_WATERMARK:-0.9}" \
    --eviction_ratio="${EVICTION_RATIO:-0.1}" \
    --allocation_strategy="${ALLOCATION_STRATEGY:-random}" \
    --enable_http_metadata_server=true \
    --http_metadata_server_host="$MASTER_HOST" \
    --http_metadata_server_port="$METADATA_PORT" \
    --metrics_port="$METRICS_PORT" \
    --enable_ha=false \
    --logtostderr=true \
    --minloglevel="$MINLOGLEVEL" \
    > "$LOG_DIR/master.log" 2>&1 &
  echo $! > "$(pidfile master)"
  if ! wait_port; then
    echo "master did not become ready" >&2
    tail -40 "$LOG_DIR/master.log" >&2 || true
    exit 1
  fi
  metrics_dump
}
start_segment() {
  common_env
  mkdir -p "$LOG_DIR"
  local name="${1:-}"
  [[ -n "$name" ]] || die "usage: $0 segment <role>"
  local i; i="$(role_index "$name")" || die "role '$name' not in $CONF"
  if is_running "holder-$name"; then
    note "holder $name already running pid=$(cat "$(pidfile "holder-$name")")"
    return 0
  fi
  note "segment $name hostname=${LOCAL_HOSTNAME[$i]} devices=${DEVICES[$i]:-auto} size=$(SEGMENT_SIZE_OF_ROLE "$name")"
  echo "=== start $(date -Is) profile=$PROFILE" >> "$LOG_DIR/holder-$name.log"
  local offset; offset="$(wc -l < "$LOG_DIR/holder-$name.log")"
  local close_args=()
  if [[ "$(basename "$HOLDER")" == "holder.py" ]]; then
    close_args=(--close-timeout "$HOLDER_CLOSE_TIMEOUT")
  fi
  python3 "$HOLDER" \
    --master-server "$MASTER_SERVER" \
    --metadata-server "$METADATA_SERVER" \
    --local-hostname "${LOCAL_HOSTNAME[$i]}" \
    --protocol rdma \
    --device-names "${DEVICES[$i]}" \
    --global-segment-size "$(SEGMENT_SIZE_OF_ROLE "$name")" \
    "${close_args[@]}" \
    >> "$LOG_DIR/holder-$name.log" 2>&1 &
  echo $! > "$(pidfile "holder-$name")"
  local ready=0
  for _ in $(seq 1 180); do
    if tail -n "+$((offset + 1))" "$LOG_DIR/holder-$name.log" 2>/dev/null |
       grep -q "holder_ready"; then
      ready=1; break
    fi
    if ! kill -0 "$(cat "$(pidfile "holder-$name")")" 2>/dev/null; then
      echo "holder $name exited during setup" >&2
      tail -40 "$LOG_DIR/holder-$name.log" >&2 || true
      exit 1
    fi
    sleep 1
  done
  if [[ "$ready" != 1 ]]; then
    tail -40 "$LOG_DIR/holder-$name.log" >&2 || true
    die "holder $name not ready"
  fi
  tail -1 "$LOG_DIR/holder-$name.log"
}

start_probe() {
  common_env
  mkdir -p "$LOG_DIR"
  local i; i="$(role_index writer)" || die "nodes.conf needs a writer role for the probe defaults"
  local lh="${PROBE_LOCAL_HOSTNAME:-${LOCAL_HOSTNAME[$i]}}"
  local dev="${PROBE_DEVICES:-${DEVICES[$i]%%,*}}"
  if is_running probe; then
    note "probe already running pid=$(cat "$(pidfile probe)")"
    return 0
  fi
  note "probe hostname=$lh device=$dev"
  python3 "$PROBE" \
    --master-server "$MASTER_SERVER" \
    --metadata-server "$METADATA_SERVER" \
    --local-hostname "$lh" \
    --protocol rdma \
    --device-names "$dev" \
    --interval "$PROBE_INTERVAL" \
    --warn-ms "$PROBE_WARN_MS" \
    > "$LOG_DIR/probe.log" 2>&1 &
  echo $! > "$(pidfile probe)"
  sleep 2
  if ! kill -0 "$(cat "$(pidfile probe)")" 2>/dev/null; then
    tail -40 "$LOG_DIR/probe.log" >&2
    die "probe exited"
  fi
  note "probe pid=$(cat "$(pidfile probe)")"
}


start_writer() {
  common_env
  mkdir -p "$LOG_DIR"
  local i; i="$(role_index writer)" || die "nodes.conf needs a writer role"
  [[ -f "$LOADER" ]] || die "loader not found: $LOADER"
  if is_running loader; then
    note "writer already running pid=$(cat "$(pidfile loader)")"
    return 0
  fi
  note "writer hostname=${LOCAL_HOSTNAME[$i]} devices=${DEVICES[$i]:-auto} workers=$WORKERS batch=$BATCH_SIZE value=$VALUE_SIZE max_keys=$MAX_KEYS"
  python3 "$LOADER" \
    --master-server "$MASTER_SERVER" \
    --metadata-server "$METADATA_SERVER" \
    --local-hostname "${LOCAL_HOSTNAME[$i]}" \
    --protocol rdma \
    --device-names "${DEVICES[$i]}" \
    --global-segment-size "${SEGMENT_SIZE_WRITER:-0}" \
    --value-size "$VALUE_SIZE" \
    --batch-size "$BATCH_SIZE" \
    --workers "$WORKERS" \
    --max-keys "$MAX_KEYS" \
    --key-prefix "$KEY_PREFIX" \
    > "$LOG_DIR/loader.log" 2>&1 &
  echo $! > "$(pidfile loader)"
  sleep 2
  if ! kill -0 "$(cat "$(pidfile loader)")" 2>/dev/null; then
    tail -60 "$LOG_DIR/loader.log" >&2
    die "writer exited"
  fi
  note "writer pid=$(cat "$(pidfile loader)")"
}

count_lines() {  # count_lines <pattern> <file>: always exactly one number
  local n
  n=$(grep -c "$1" "$2" 2>/dev/null) || true
  echo "${n:-0}"
}

status() {
  local now; now="$(date -Is)"
  echo "== $now host=$(hostname) profile=$PROFILE =="
  echo "-- processes --"
  local name f pid
  for name in master probe loader; do
    f="$(pidfile "$name")"
    if [[ -f "$f" ]] && kill -0 "$(cat "$f")" 2>/dev/null; then
      pid="$(cat "$f")"
      ps -o pid,etime,rss,stat,args -p "$pid" | tail -n +2 | cut -c1-140
    fi
  done
  for name in $(holder_roles); do
    f="$(pidfile "holder-$name")"
    if [[ -f "$f" ]] && kill -0 "$(cat "$f")" 2>/dev/null; then
      pid="$(cat "$f")"
      printf 'holder-%-8s ' "$name"
      ps -o pid,etime,rss,stat -p "$pid" | tail -n +2
    fi
  done
  echo "-- master metrics --"
  metrics_dump
  echo "-- master log --"
  grep -E "\[EVICT-TRIGGER\]|\[EVICT-DONE\]|client_liveness|client_offboarding|action=mount_segment|action=remount|Unmount" \
    "$LOG_DIR/master.log" 2>/dev/null | tail -12 || true
  echo "-- probe stalls --"
  count_lines '"event": "stall"' "$LOG_DIR/probe.log"
  grep '"event": "stall"' "$LOG_DIR/probe.log" 2>/dev/null | tail -3 || true
  echo "-- writer --"
  count_lines "Connection timed out" "$LOG_DIR/loader.log"
  grep '"event": "progress"\\|"event": "complete"' "$LOG_DIR/loader.log" 2>/dev/null | tail -2 || true
  echo "-- memory --"
  free -g | head -2
}

wait_evict() {
  local timeout="${1:-3600}"
  mkdir -p "$LOG_DIR"
  note "waiting for [EVICT-TRIGGER] in $LOG_DIR/master.log (timeout ${timeout}s)"
  for _ in $(seq 1 "$timeout"); do
    if grep -q "\[EVICT-TRIGGER\]" "$LOG_DIR/master.log" 2>/dev/null; then
      grep -E "\[EVICT-TRIGGER\]|\[EVICT-DONE\]" "$LOG_DIR/master.log" | tail -5
      note "eviction window is open; run: $0 restart-holder <holder1|holder2>"
      return 0
    fi
    if [[ -f "$(pidfile master)" ]] && ! kill -0 "$(cat "$(pidfile master)")" 2>/dev/null; then
      die "master exited while waiting for eviction"
    fi
    sleep 1
  done
  die "no [EVICT-TRIGGER] within ${timeout}s; check capacity and writer progress"
}

restart_holder() {
  common_env
  local name="${1:-}"
  [[ -n "$name" ]] || die "usage: $0 restart-holder <role>"
  role_index "$name" >/dev/null || die "role '$name' not in $CONF"
  is_running "holder-$name" || die "holder $name is not running"
  local before; before="$(date -Is)"
  if [[ "${RESTART_MODE:-term}" == "kill" ]]; then
    note "killing holder $name at $before (SIGKILL, no unmount RPC)"
    kill -KILL "$(cat "$(pidfile "holder-$name")")" 2>/dev/null || true
    rm -f "$(pidfile "holder-$name")"
  else
    note "restarting holder $name at $before (SIGTERM -> close/unmount, grace ${STOP_GRACE}s)"
    stop_pidfile "holder-$name" "$STOP_GRACE"
  fi
  sleep 1
  start_segment "$name"
  note "holder $name restarted; watch status for 60-180s"
}

stack() {
  common_env
  local pid out
  [[ -f "$(pidfile master)" ]] || die "master is not running"
  pid="$(cat "$(pidfile master)")"
  mkdir -p "$LOG_DIR"
  out="$LOG_DIR/master-stacks-$(date +%H%M%S).txt"
  gdb -p "$pid" -batch \
    -ex 'set pagination off' \
    -ex 'thread apply all bt' > "$out" 2>&1 || true
  echo "wrote $out"
  grep -n -E "MasterService::Ping|MasterService::ReMountSegment|MasterService::MountSegment|MasterService::BatchEvict|MasterService::ClearStaleHandles|ProcessClientOffboardingJob|client_mutex_|master_state_mutex_" \
    "$out" | head -80 || true
  if grep -q "ptrace: Operation not permitted\\|Could not attach" "$out"; then
    echo "gdb attach denied; set /proc/sys/kernel/yama/ptrace_scope=0 or run as root" >&2
  fi
}

stop_all() {
  local keep_master="${1:-}"
  local name
  stop_pidfile loader "$STOP_GRACE"
  for name in $(holder_roles); do
    stop_pidfile "holder-$name" "$STOP_GRACE"
  done
  stop_pidfile probe 5
  [[ "$keep_master" == "--keep-master" ]] || stop_pidfile master 20
  note "stopped (logs in $LOG_DIR)"
}

collect() {
  mkdir -p "$BASE/logs"
  local stamp out
  stamp="$(date +%Y%m%d-%H%M%S)"
  out="$BASE/logs/4node-$PROFILE-$(hostname)-$stamp.tgz"
  {
    echo "host=$(hostname)"
    echo "profile=$PROFILE"
    echo "date=$(date -Is)"
    free -g
    nproc
    hostname -I
    ls /sys/class/infiniband 2>/dev/null || true
  } > "$LOG_DIR/node-info.txt" 2>&1 || true
  metrics_dump > "$LOG_DIR/metrics.txt" 2>&1 || true
  tar czf "$out" -C "$(dirname "$LOG_DIR")" "$(basename "$LOG_DIR")" 2>/dev/null ||
    tar czf "$out" -C "$LOG_DIR" . 2>/dev/null || true
  echo "$out"
}

preflight() {
  common_env
  echo "== preflight host=$(hostname) profile=$PROFILE =="
  echo "-- paths --"
  echo "workspace=$WORKSPACE"
  [[ -d "$WORKSPACE" ]] || die "/workspace is not mounted"
  echo "fstype /workspace: $(stat -f -c %T "$WORKSPACE" 2>/dev/null || echo unknown)"

  echo "-- runtime --"
  if [[ "$RUNTIME_DIR" == "-" ]]; then
    echo "runtime: installed site-packages"
  elif [[ -d "$RUNTIME_DIR/mooncake" ]]; then
    echo "runtime: staged $RUNTIME_DIR"
  else
    echo "runtime: PREREQ MISSING ($RUNTIME_DIR/mooncake not found)"
  fi
  python3 -V
  python3 -c 'import mooncake; print("import:", mooncake.__file__)' 2>/dev/null || true
  python3 - <<'PY' 2>/dev/null || true
import importlib.metadata as md
for d in md.distributions():
    try:
        n = d.metadata.get("Name") or ""
    except Exception:
        continue
    if "mooncake" in n.lower():
        print("installed package: %s==%s" % (n, d.version))
PY
  if python3 -c 'from mooncake.store import MooncakeDistributedStore' 2>/tmp/mc4-import.err; then
    echo "python binding: OK"
    python3 -c 'import mooncake.version as v; print("version:", getattr(v, "__hcu_version__", None) or getattr(v, "__version__", "?"))' 2>/dev/null || true
  else
    echo "python binding: FAIL"
    tail -3 /tmp/mc4-import.err 2>/dev/null || true
  fi
  local bin; bin="$(master_bin)"
  echo "master: $bin"
  "$bin" --version 2>&1 | head -1 || true

  echo "-- memory --"
  free -g | head -3

  echo "-- rdma --"
  ls /sys/class/infiniband 2>/dev/null || echo "no /sys/class/infiniband"
  command -v ibv_devinfo >/dev/null && ibv_devinfo -l 2>/dev/null | head -12 || true
  echo "ulimit -l: $(ulimit -l) (raise to unlimited if RDMA registration fails)"
  echo "ulimit -n: $(ulimit -n)"

  echo "-- ports --"
  if [[ "$MASTER_HOST" == "127.0.0.1" || "$MASTER_HOST" == "localhost" ]] ||
     hostname -I 2>/dev/null | tr ' ' '\n' | grep -qx "$MASTER_HOST"; then
    if port_free "$MASTER_HOST" "$MASTER_PORT"; then
      echo "$MASTER_HOST:$MASTER_PORT free (local master)"
    else
      echo "WARN $MASTER_HOST:$MASTER_PORT busy (another master?)"
    fi
    echo "localhost:$METRICS_PORT $(port_free 127.0.0.1 "$METRICS_PORT" && echo free || echo busy)"
  else
    echo "master $MASTER_HOST:$MASTER_PORT is remote; not checked on this node"
  fi

  echo "-- proxy --"
  env | grep -iE '^(http|https|all|no)_proxy=' || echo "no proxy variables"

  echo "-- this node role --"
  local me i host
  me="$(hostname -s)"
  for i in "${!ROLE[@]}"; do
    host="${SSH_HOST[$i]}"
    if [[ "$host" == "$me" || "$host" == "$(hostname)" ]] || hostname -I 2>/dev/null | grep -qw "${LOCAL_HOSTNAME[$i]}"; then
      echo "role=${ROLE[$i]} local_hostname=${LOCAL_HOSTNAME[$i]} devices=${DEVICES[$i]:-auto}"
    fi
  done
  note "preflight done"
}

usage() {
  cat >&2 <<'EOF'
Usage: mc4.sh <command> [args]

  preflight                  check this node (paths, binding, RDMA, ports, memory)
  master                     start mooncake_master
  segment <role>             start one segment role (holder1, holder2, ...)
  probe                      start probe_exist.py (no segment)
  writer                     start loader.py (owns the writer segment)
  wait-evict [timeout_s]     wait for [EVICT-TRIGGER]
  restart-holder <role>      TERM/KILL + restart one holder (docker stop equivalent)
  status                     pids, metrics, eviction/liveness lines, stalls, -901
  stack                      gdb thread apply all bt on the master
  collect                    tar logs + metrics to ../logs
  stop [--keep-master]       writer -> holders -> probe -> master

Environment:
  PROFILE=product|deterministic  (default product: 16/16 threads; deterministic: 1/1)
  CONF=/path/nodes.conf          (default ./nodes.conf)
  SEGMENT_SIZE=64GiB VALUE_SIZE=64 BATCH_SIZE=12288 WORKERS=128
  CLIENT_TTL=120 LEASE=1h MAX_KEYS=0
  RPC_THREAD_NUM HANDLER_WORKER_THREAD_NUM MASTER_PORT METADATA_PORT METRICS_PORT
  LOG_DIR=/tmp/mc4-$PROFILE RUNTIME_DIR=- (installed package) or /path/to/staged
EOF
  exit 1
}

load_conf
common_env
cmd="${1:-}"; shift || true
case "$cmd" in
  preflight) preflight ;;
  master) start_master ;;
  segment) start_segment "${1:-}" ;;
  probe) start_probe ;;
  writer) start_writer ;;
  wait-evict) wait_evict "${1:-3600}" ;;
  restart-holder) restart_holder "${1:-}" ;;
  status) status ;;
  stack) stack ;;
  collect) collect ;;
  stop) stop_all "${1:-}" ;;
  *) usage ;;
esac

