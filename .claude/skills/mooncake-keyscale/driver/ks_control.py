#!/usr/bin/env python3
"""ks_control.py - driver for the Mooncake key-scale / eviction-stability test.

    python3 ks_control.py check 6
    python3 ks_control.py plan 6
    python3 ks_control.py preflight 6
    python3 ks_control.py start 6
    python3 ks_control.py watch 6 [rounds]
    python3 ks_control.py status 6
    python3 ks_control.py report 6 [run_id]
    python3 ks_control.py stop 6
    python3 ks_control.py run 6 [rounds]
    python3 ks_control.py smoke 6

Runs on any machine with python3 and ssh access to every node in the conf.
The host only drives: every role and every file lives inside the per-node
container, reached with `ssh <host> docker exec <container> ...`.

No shared filesystem is assumed. The conf, run.env and the role scripts are
pushed into each container's own /tmp/ks/<run_id>/, the roles run and log
there, and at report time each container's logs are streamed back into the
master container, where report.py renders the report.

This driver does NOT build or distribute packages; check/preflight only check
what is already installed inside each container.
"""

import io
import os
import posixpath
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import time

# ---------------------------------------------------------------- settings

SSH_OPTS = os.environ.get(
    "SSH_OPTS",
    "-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no",
).split()
# Which mooncake package the run uses:
#   "-"     the package installed in each container (site-packages) -- default
#   <dir>   a staged tree inside the container (RUNTIME_DIR/mooncake) -- opt-in only
RUNTIME_DIR = os.environ.get("RUNTIME_DIR", "-")

SEGMENT_SIZE = os.environ.get("SEGMENT_SIZE", "40GiB")
VALUE_SIZE = os.environ.get("VALUE_SIZE", "64")
BATCH_SIZE = os.environ.get("BATCH_SIZE", "12288")
WORKERS = os.environ.get("WORKERS", "128")
CLIENT_TTL = os.environ.get("CLIENT_TTL", "120")
LEASE = os.environ.get("LEASE", "1h")
MASTER_PORT = os.environ.get("MASTER_PORT", "50551")
METADATA_PORT = os.environ.get("METADATA_PORT", "38080")
METRICS_PORT = os.environ.get("METRICS_PORT", "39003")
PROFILE = os.environ.get("PROFILE", "product")
SAMPLE_INTERVAL = os.environ.get("SAMPLE_INTERVAL", "15")
EVICT_ROUNDS = int(os.environ.get("EVICT_ROUNDS", "8"))
EVICTION_RATIO = os.environ.get("EVICTION_RATIO", "0.1")
EVICTION_HIGH_WATERMARK = os.environ.get("EVICTION_HIGH_WATERMARK", "0.9")
ALLOCATION_STRATEGY = os.environ.get("ALLOCATION_STRATEGY", "random")
FILL_TIMEOUT = int(os.environ.get("FILL_TIMEOUT", "7200"))
ROUND_TIMEOUT = int(os.environ.get("ROUND_TIMEOUT", "1800"))
MEM_HEADROOM_GIB = int(os.environ.get("MEM_HEADROOM_GIB", "30"))
RSS_PER_KEY = int(os.environ.get("RSS_PER_KEY", "1210"))
RUNS_ROOT = os.environ.get("RUNS_ROOT", "/workspace/mooncake-realkey/keyscale-runs")
ARTIFACT_OWNER = os.environ.get("ARTIFACT_OWNER", "")

SELF_DIR = os.path.dirname(os.path.abspath(__file__))   # <skill>/driver
SKILL_ROOT = os.path.dirname(SELF_DIR)                  # <skill>
RUNTIME_SRC = os.path.join(SKILL_ROOT, "runtime")       # files pushed into containers
CONF_DIR = os.path.join(SKILL_ROOT, "conf")

SEG_KEYS_CAP = 64 * 1024 * 1024


def die(msg):
    sys.stderr.write("ERROR: %s\n" % msg)
    sys.exit(1)


def note(msg):
    print("[ks] %s" % msg, flush=True)


# ---------------------------------------------------------------- conf


class Row(object):
    __slots__ = ("role", "host", "container", "ip", "devices", "size")

    def __init__(self, role, host, container, ip, devices, size=""):
        self.role = role
        self.host = host
        self.container = container
        self.ip = ip
        self.devices = devices
        self.size = size


class Conf(object):
    def __init__(self, path, rows, nseg):
        self.path = path
        self.rows = rows
        self.nseg = nseg

    def row(self, role):
        for r in self.rows:
            if r.role == role:
                return r
        die("no role '%s' in %s" % (role, self.path))

    def holders(self):
        return [r for r in self.rows if r.role not in ("master", "writer")]

    def pairs(self):
        seen, out = set(), []
        for r in self.rows:
            k = (r.host, r.container)
            if k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    def hosts(self):
        seen, out = set(), []
        for r in self.rows:
            if r.host in seen:
                continue
            seen.add(r.host)
            out.append(r.host)
        return out

    def text(self):
        with open(self.path, encoding="utf-8") as fh:
            return fh.read()

    def short_name(self):
        name = os.path.basename(self.path)
        if name.startswith("nodes.") and name.endswith(".conf"):
            name = name[len("nodes."):-len(".conf")]
        return name


SIZE_OVERRIDE = ""


def default_size():
    return SIZE_OVERRIDE or SEGMENT_SIZE


def row_size(row):
    """Explicit per-segment size from the conf, else the global default."""
    return SIZE_OVERRIDE or row.size or SEGMENT_SIZE


def row_gib(row):
    return size_to_gib(row_size(row))


def resolve_conf(target):
    """Accept a path, a bare name in conf/, or just the build name (6seg)."""
    if not target:
        env = os.environ.get("CONF_OVERRIDE", "")
        if env:
            return env
        die("conf file required, e.g. conf/nodes.6seg.conf")
    if os.path.isfile(target):
        return target
    base = os.path.basename(target)
    if base.endswith(".conf"):
        base = base[:-5]
    for name in (base + ".conf", "nodes." + base + ".conf", "nodes." + base + "seg.conf"):
        cand = os.path.join(CONF_DIR, name)
        if os.path.isfile(cand):
            return cand
    die("conf not found: %s (looked in %s)" % (target, CONF_DIR))


SIZE_RE = re.compile(r"^\d+(?:\.\d+)?\s*[A-Za-z]{0,3}$")


def load_conf(path):
    if not os.path.isfile(path):
        die("cannot read conf: %s" % path)
    rows = []
    with open(path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            parts = s.split()
            if len(parts) < 4 or len(parts) > 6:
                die("%s:%d: need 4-6 columns "
                    "(role ssh_host container local_hostname [devices] [segment_size]); "
                    "devices may be omitted for auto-discovery" % (path, lineno))
            role, host, ct, ip = parts[:4]
            devices, size = "", ""
            if len(parts) == 6:
                devices, size = parts[4], parts[5]
            elif len(parts) == 5:
                # 5th column is the segment size when it looks like one,
                # otherwise it is a device list
                if SIZE_RE.match(parts[4]):
                    size = parts[4]
                else:
                    devices = parts[4]
            if devices == "-":
                devices = ""
            if size == "-":
                size = ""
            if ct == "-":
                die("%s:%d: the container column is required (there is no default container)"
                    % (path, lineno))
            rows.append(Row(role, host, ct, ip, devices, size))
    if not rows:
        die("%s has no entries" % path)
    # the writer is a pure client and owns no segment: segments == holders
    nseg = len([r for r in rows if r.role not in ("master", "writer")])
    return Conf(path, rows, nseg)


# ---------------------------------------------------------------- exec


DRIVER_MODE = os.environ.get("DRIVER", "master")  # master | host | local | auto


def is_local(host):
    """May we reach this node with `docker exec` directly, without ssh?"""
    if DRIVER_MODE == "local":
        return False
    if host in ("localhost", "127.0.0.1"):
        return True
    me = {socket.gethostname(), socket.gethostname().split(".")[0]}
    return host in me


def _docker_argv(host, container, need_stdin):
    if is_local(host):
        base = ["docker", "exec"]
        if need_stdin:
            base.append("-i")
        return base + [container]
    base = ["ssh"] + SSH_OPTS
    if not need_stdin:
        base.append("-n")
    base += [host, "docker", "exec"]
    if need_stdin:
        base.append("-i")
    return base + [container]


def bash_in(host, container, script, check=True):
    """Run a bash script inside the container; script goes over stdin (no quoting)."""
    argv = _docker_argv(host, container, True) + ["bash", "-s"]
    p = subprocess.run(
        argv, input=script.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out = p.stdout.decode(errors="replace")
    err = p.stderr.decode(errors="replace")
    if check and p.returncode != 0:
        sys.stderr.write(out)
        sys.stderr.write(err)
        die("command failed on %s/%s (rc=%d)" % (host, container, p.returncode))
    return p.returncode, out, err


# ---------------------------------------------------------------- file push


def _add_bytes(tf, arcname, data, mode=0o644):
    ti = tarfile.TarInfo(arcname)
    ti.size = len(data)
    ti.mode = mode
    ti.mtime = int(time.time())
    tf.addfile(ti, io.BytesIO(data))


def _add_tree(tf, local_dir, prefix):
    for root, _dirs, files in os.walk(local_dir):
        for name in sorted(files):
            full = os.path.join(root, name)
            rel = os.path.relpath(full, local_dir).replace(os.sep, "/")
            with open(full, "rb") as fh:
                data = fh.read()
            mode = 0o755 if name.endswith((".sh", ".py")) else 0o644
            _add_bytes(tf, posixpath.join(prefix, rel), data, mode)


def build_payload(conf, env_text, with_conf=True, with_env=True):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        _add_tree(tf, RUNTIME_SRC, "")
        if with_env:
            _add_bytes(tf, "run.env", env_text.encode(), 0o644)
        if with_conf:
            _add_bytes(tf, "conf/nodes.conf", conf.text().encode(), 0o644)
    return buf.getvalue()


def env_text(conf, run_id, work, rounds=None):
    lines = [
        "# generated by ks_control.py at %s for run %s" % (time.strftime("%Y-%m-%dT%H:%M:%S%z"), run_id),
        "PROFILE=%s" % PROFILE,
        "EVICT_ROUNDS=%s" % (rounds if rounds else EVICT_ROUNDS),
        "SEGMENT_SIZE=%s" % default_size(),
        "VALUE_SIZE=%s" % VALUE_SIZE,
        "BATCH_SIZE=%s" % BATCH_SIZE,
        "WORKERS=%s" % WORKERS,
        "MAX_KEYS=0",
        "CLIENT_TTL=%s" % CLIENT_TTL,
        "LEASE=%s" % LEASE,
        "KEY_PREFIX=ks%d" % conf.nseg,
        "MASTER_PORT=%s" % MASTER_PORT,
        "METADATA_PORT=%s" % METADATA_PORT,
        "METRICS_PORT=%s" % METRICS_PORT,
        "RUNTIME_DIR=%s" % RUNTIME_DIR,
        "SAMPLE_INTERVAL=%s" % SAMPLE_INTERVAL,
        "EVICTION_RATIO=%s" % EVICTION_RATIO,
        "EVICTION_HIGH_WATERMARK=%s" % EVICTION_HIGH_WATERMARK,
        "ALLOCATION_STRATEGY=%s" % ALLOCATION_STRATEGY,
        "STOP_GRACE=10",
        "HOLDER_CLOSE_TIMEOUT=10",
        "LOG_DIR=%s/logs" % work,
        "",
    ]
    # mc4.sh reads SEGMENT_SIZE_<ROLE>; one line per segment that sets its own size
    for r in conf.rows:
        if r.role == "master" or not r.size or SIZE_OVERRIDE:
            continue
        lines.insert(-1, "SEGMENT_SIZE_%s=%s" % (r.role.upper(), r.size))
    return "\n".join(lines)


def push(host, container, payload, work, wipe=True):
    if wipe:
        bash_in(host, container, "rm -rf %s && mkdir -p %s/conf %s/logs" % (work, work, work))
    else:
        bash_in(host, container, "mkdir -p %s" % work)
    argv = _docker_argv(host, container, True) + ["tar", "-xzf", "-", "-C", work]
    p = subprocess.run(argv, input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
        sys.stderr.write(p.stderr.decode(errors="replace"))
        die("push failed to %s/%s" % (host, container))


# ---------------------------------------------------------------- roles


def mc4(conf, row, *args):
    cmd = " ".join(shlex.quote(a) for a in args)
    script = (
        "cd {w} && CONF={w}/conf/nodes.conf MC4_ENV_FILE={w}/run.env "
        "RUNTIME_DIR={rt} LOG_DIR={w}/logs bash {w}/mc4.sh {cmd}"
    ).format(w=conf.work, rt=RUNTIME_DIR, cmd=cmd)
    rc, out, err = bash_in(row.host, row.container, script)
    sys.stdout.write(out)
    if err.strip():
        sys.stderr.write(err)
    return rc


# ---------------------------------------------------------------- check


def probe(conf, host, container):
    script = "RUNTIME_DIR=%s python3 %s/envprobe.py" % (RUNTIME_DIR, conf.work)
    _rc, out, _err = bash_in(host, container, script, check=False)
    lines = [l for l in out.strip().splitlines() if l.strip()]
    return lines[-1] if lines else ""


def where(conf):
    """Print where this conf expects to be driven from, without touching anything."""
    m = conf.row("master")
    name = conf.short_name()
    print("conf        : %s" % conf.path)
    print("segments    : %d holders (writer is a pure client, no segment)" % conf.nseg)
    print("master node : %s (container %s)" % (m.host, m.container))
    for host in conf.hosts():
        rows = [r for r in conf.rows if r.host == host]
        ct = rows[0].container
        roles = ", ".join(r.role for r in rows)
        print("  %-12s %-16s %s" % (host, ct, roles))
    print()
    print("run it on the master host:")
    print("  ssh %s \"python3 %s check %s\"" % (m.host, os.path.abspath(__file__), name))
    print("  ssh %s \"python3 %s run %s 8\"" % (m.host, os.path.abspath(__file__), name))


def check(conf, quiet=False):
    payload = build_payload(conf, env_text(conf, conf.run_id, conf.work))
    rows_out = []
    ref = None
    bad = []
    if not quiet:
        print("=== check: %d segments / %d data clients ===" % (conf.nseg, conf.nseg))
        print("driver mode: %s" % DRIVER_MODE)
        print("runtime: %s" % ("installed site-packages (RUNTIME_DIR=-)"
                              if RUNTIME_DIR in ("", "-") else "staged %s" % RUNTIME_DIR))
        print("%-12s %-9s %-44s %-9s %-12s %-4s %s"
              % ("host", "python", "installed package", "origin", "master-git", "hca", "ulimit"))
    for host, ct in conf.pairs():
        push(host, ct, payload, conf.work)
        line = probe(conf, host, ct)
        if not line:
            if not quiet:
                print("%-12s %-9s %-44s %-9s %-12s %-4s %s" % (host, "?", "?", "?", "?", "0", "?"))
            bad.append("%s/%s:cannot-probe" % (host, ct))
            continue
        p = line.split("|")
        if len(p) < 9:
            bad.append("%s/%s:bad-probe" % (host, ct))
            continue
        _h, py, pkg, modver, origin, git, hca, ulim, _master = p[:9]
        rtstate = p[9] if len(p) > 9 else "off"
        if not quiet:
            print("%-12s %-9s %-44s %-9s %-12s %-4s %s"
                  % (_h, py, pkg, origin, git, hca, ulim))
        rows_out.append((host, py, pkg, git, origin, hca))
        if ref is None:
            ref = (py, pkg, git)
        if py != ref[0]:
            bad.append("%s:python(%s!=%s)" % (host, py, ref[0]))
        if pkg != ref[1]:
            bad.append("%s:package(%s!=%s)" % (host, pkg, ref[1]))
        if git != ref[2]:
            bad.append("%s:master-git(%s!=%s)" % (host, git, ref[2]))
        if origin == "MISSING":
            bad.append("%s/%s:no-mooncake-import" % (host, ct))
        if rtstate == "missing":
            bad.append("%s/%s:RUNTIME_DIR-not-found(%s)" % (host, ct, RUNTIME_DIR))
        if _master == "-":
            bad.append("%s/%s:no-master-binary" % (host, ct))
        # a staged tree must be exactly what is installed, otherwise `check` would
        # be validating a package the run never imports
        installed_ver = pkg.split("==")[-1] if "==" in pkg else pkg
        if origin == "staged" and modver not in ("?", "-") and installed_ver != modver:
            bad.append("%s:staged(%s)!=installed(%s)" % (host, modver, installed_ver))
        if not hca.isdigit() or int(hca) <= 0:
            bad.append("%s:no-rdma-device" % host)
    if not bad and ref:
        print("consistency: OK (python=%s installed-package=%s master-git=%s)" % ref)
        return 0
    print("consistency: MISMATCH")
    print("PREREQ MISSING: %s" % " ".join(bad))
    print("the skill does not build or distribute packages; install the same wheel in every container,"
          " or point RUNTIME_DIR at an identical staged tree on every node")
    return 1


# ---------------------------------------------------------------- plan


def size_to_gib(text):
    m = re.match(r"^([0-9.]+)\s*([A-Za-z]*)$", text.strip())
    if not m:
        die("bad size: %s" % text)
    n = float(m.group(1))
    u = (m.group(2) or "B").upper()
    mul = {
        "B": 1, "K": 1e3, "KB": 1e3, "KI": 1024, "KIB": 1024,
        "M": 1e6, "MB": 1e6, "MI": 1024 ** 2, "MIB": 1024 ** 2,
        "G": 1e9, "GB": 1e9, "GI": 1024 ** 3, "GIB": 1024 ** 3,
    }
    if u not in mul:
        die("bad size unit: %s" % text)
    return n * mul[u] / 1024 ** 3


def keys_per_segment(sgib):
    return min(int(sgib * 1024 ** 3) // 1024, SEG_KEYS_CAP)


def node_avail_gib(conf, host):
    ct = next((r.container for r in conf.rows if r.host == host), None)
    if not ct:
        return None
    _rc, out, _err = bash_in(
        host, ct, "free -g | awk 'NR==2{print $7}'", check=False
    )
    digits = re.sub(r"[^0-9]", "", out)
    return int(digits) if digits else None


def plan(conf):
    segs = [r for r in conf.rows if r.role not in ("master", "writer")]
    gibs = [(r, row_gib(r)) for r in segs]
    keys = sum(keys_per_segment(g) for _r, g in gibs)
    master_host = conf.row("master").host
    uniq = sorted(set(row_size(r) for r in segs))

    print("=== plan: %d segments (= %d data clients) ===" % (conf.nseg, conf.nseg))
    print("conf             : %s" % conf.path)
    print("segment size     : %s" % (uniq[0] if len(uniq) == 1 else "mixed: " + ", ".join(uniq)))
    print("writer           : no segment (pure client), on %s" % conf.row("writer").host)
    print("resident cap     : %s keys" % format(keys, ","))
    print("master RSS est.  : %.0f GiB  (at %d B/key)" % (keys * RSS_PER_KEY / 1024 ** 3, RSS_PER_KEY))
    if len(uniq) > 1:
        print()
        print("--- per-segment layout ---")
        for r, g in gibs:
            print("  %-12s %-16s %-9s %-9s %s keys"
                  % (r.host, r.container, r.role, row_size(r), format(keys_per_segment(g), ",")))
    print()
    print("--- per-node memory check (available, GiB) ---")
    for host in conf.hosts():
        avail = node_avail_gib(conf, host)
        if avail is None:
            print("  %-12s UNREACHABLE (ssh %s + docker exec)" % (host, host))
            continue
        mine = [g for r, g in gibs if r.host == host]
        segs_gib = sum(mine)
        if host == master_host:
            need = int(keys * RSS_PER_KEY / 1024 ** 3 + segs_gib)
            label = "master+%dseg" % len(mine)
        else:
            need = int(segs_gib)
            label = "%dseg" % len(mine)
        verdict = "OK" if avail >= need + MEM_HEADROOM_GIB else "TOO TIGHT"
        print("  %-12s role=%-12s need=%4d  avail=%4d  %s" % (host, label, need, avail, verdict))
    print()
    print("note: a segment caps at min(bytes/1024, 64Mi) keys.")
    if len(uniq) > 1:
        print("      sizes come from column 6 of the conf; rows without it use %s." % SEGMENT_SIZE)
    else:
        print("      %d segments x %s needs ~%d GiB master RSS."
              % (conf.nseg, SEGMENT_SIZE, int(keys * RSS_PER_KEY / 1024 ** 3)))


# ---------------------------------------------------------------- preflight


def preflight(conf):
    payload = build_payload(conf, env_text(conf, conf.run_id, conf.work))
    for host, ct in conf.pairs():
        print("########## preflight %s on %s ##########" % (ct, host))
        push(host, ct, payload, conf.work)
        row = next(r for r in conf.rows if r.host == host and r.container == ct)
        mc4(conf, row, "preflight")
    print("########## package consistency ##########")
    check(conf, quiet=True)


# ---------------------------------------------------------------- start/stop


def start_sampler(conf):
    m = conf.row("master")
    script = (
        "mkdir -p {w}/logs; cd {w}/logs && nohup setsid python3 {w}/sampler.py "
        "--metrics-url http://127.0.0.1:{mp}/metrics "
        "--master-pidfile {w}/logs/master.pid --out {w}/logs/sampler.csv "
        "--interval {iv} > {w}/logs/sampler.log 2>&1 < /dev/null & sleep 1; echo sampler_started"
    ).format(w=conf.work, mp=METRICS_PORT, iv=SAMPLE_INTERVAL)
    bash_in(m.host, m.container, script)


def stop_sampler(conf):
    m = conf.row("master")
    bash_in(m.host, m.container, "pkill -f 'sampler.py --metrics-url' 2>/dev/null; true", check=False)


def record_rounds(conf, rounds):
    """Persist the round count so a later standalone `report` clips the same way."""
    m = conf.row("master")
    bash_in(
        m.host, m.container,
        "cd {w} && (grep -q '^EVICT_ROUNDS=' run.env && "
        "sed -i 's/^EVICT_ROUNDS=.*/EVICT_ROUNDS={n}/' run.env || "
        "echo 'EVICT_ROUNDS={n}' >> run.env)".format(w=conf.work, n=rounds),
        check=False,
    )


def start(conf, rounds=None):
    payload = build_payload(conf, env_text(conf, conf.run_id, conf.work, rounds=rounds))
    for host, ct in conf.pairs():
        note("pushing run files to %s on %s" % (ct, host))
        push(host, ct, payload, conf.work)
    note("run_id=%s" % conf.run_id)
    note("container workdir: %s (inside each container)" % conf.work)
    m = conf.row("master")
    note("starting master on %s@%s" % (m.container, m.host))
    mc4(conf, m, "master")
    start_sampler(conf)
    for h in conf.holders():
        note("starting %s on %s@%s" % (h.role, h.container, h.host))
        mc4(conf, h, "segment", h.role)
    w = conf.row("writer")
    note("starting writer on %s@%s" % (w.container, w.host))
    mc4(conf, w, "writer")
    time.sleep(5)
    status(conf)


def stop(conf):
    stop_sampler(conf)
    try:
        mc4(conf, conf.row("writer"), "stop")
    except SystemExit:
        pass
    for h in conf.holders():
        try:
            mc4(conf, h, "stop")
        except SystemExit:
            pass
    try:
        mc4(conf, conf.row("master"), "stop")
    except SystemExit:
        pass
    note("stopped; per-container logs under %s/logs" % conf.work)


# ---------------------------------------------------------------- status


def _metrics_script(conf):
    return """python3 - <<'PY'
import urllib.request
try:
    t = urllib.request.urlopen('http://127.0.0.1:%s/metrics', timeout=8).read().decode()
except Exception as e:
    print('metrics_unavailable', e); raise SystemExit(0)
want = ('master_key_count ', 'master_active_clients ',
        'master_successful_evictions_total ', 'master_evicted_key_count ')
for line in t.splitlines():
    if line.startswith(want):
        print(line)
PY
""" % METRICS_PORT


def metrics(conf):
    m = conf.row("master")
    _rc, out, _err = bash_in(m.host, m.container, _metrics_script(conf), check=False)
    sys.stdout.write(out)


def metric_val(conf, name):
    m = conf.row("master")
    script = """python3 - <<'PY'
import urllib.request
try:
    t = urllib.request.urlopen('http://127.0.0.1:%s/metrics', timeout=8).read().decode()
except Exception:
    print(-1); raise SystemExit(0)
for line in t.splitlines():
    if line.startswith('%s '):
        print(int(float(line.split()[1]))); break
else:
    print(-1)
PY
""" % (METRICS_PORT, name)
    _rc, out, _err = bash_in(m.host, m.container, script, check=False)
    digits = re.sub(r"[^0-9-]", "", out)
    return int(digits) if digits not in ("", "-") else -1


def status(conf):
    m = conf.row("master")
    w = conf.row("writer")
    print("== ks status run=%s nseg=%d ==" % (conf.run_id, conf.nseg))
    metrics(conf)
    print("-- eviction lines --")
    _rc, out, _err = bash_in(
        m.host, m.container,
        "grep -E '\\[EVICT-TRIGGER\\]|\\[EVICT-DONE\\]' %s/logs/master.log 2>/dev/null | tail -6" % conf.work,
        check=False,
    )
    sys.stdout.write(out)
    print("-- writer --")
    _rc, out, _err = bash_in(
        w.host, w.container,
        "printf 'timeouts='; grep -c 'Connection timed out' %s/logs/loader.log 2>/dev/null || echo 0" % conf.work,
        check=False,
    )
    sys.stdout.write(out)
    _rc, out, _err = bash_in(
        w.host, w.container,
        "grep '\"event\": \"progress\"' %s/logs/loader.log 2>/dev/null | tail -1" % conf.work,
        check=False,
    )
    sys.stdout.write(out)
    print("-- memory --")
    for host, ct in conf.pairs():
        _rc, out, _err = bash_in(
            host, ct,
            "free -g | awk 'NR==2{printf \"total=%s used=%s avail=%s\\n\", $2, $3, $7}'",
            check=False,
        )
        print("  %-12s %-16s %s" % (host, ct, out.strip()))


# ---------------------------------------------------------------- wait/watch


def wait_fill(conf, timeout):
    cap = sum(keys_per_segment(row_gib(r)) for r in conf.rows
              if r.role not in ("master", "writer"))
    note("waiting for fill to cap %d (timeout %ds)" % (cap, timeout))
    t, last, flat = 0, 0, 0
    while t < timeout:
        kc = metric_val(conf, "master_key_count")
        if kc >= cap * 98 // 100:
            note("reached %d / %d (>=98%%)" % (kc, cap))
            return True
        if kc > 0 and kc == last:
            flat += 1
            if flat >= 6:
                note("key_count flat at %d for 6 samples; treating as full" % kc)
                return True
        else:
            flat = 0
        last = kc
        print("  t=%ds key_count=%d (%d%% of cap)" % (t, kc, kc * 100 // cap if kc > 0 else 0), flush=True)
        time.sleep(30)
        t += 30
    note("WARN fill did not reach cap within %ds (last=%d)" % (timeout, last))
    return False


def watch_evictions(conf, rounds):
    m = conf.row("master")
    note("watching %d eviction rounds" % rounds)
    for r in range(1, rounds + 1):
        t = 0
        while t < ROUND_TIMEOUT:
            _rc, out, _err = bash_in(
                m.host, m.container,
                "grep -c '\\[EVICT-DONE\\]' %s/logs/master.log 2>/dev/null || echo 0" % conf.work,
                check=False,
            )
            digits = re.sub(r"[^0-9]", "", out) or "0"
            if int(digits) >= r:
                note("round %d/%d complete (EVICT-DONE count=%d)" % (r, rounds, int(digits)))
                break
            time.sleep(30)
            t += 30
        if t >= ROUND_TIMEOUT:
            note("WARN round %d did not finish within %ds" % (r, ROUND_TIMEOUT))
            return False
    note("observed %d eviction rounds" % rounds)
    return True


# ---------------------------------------------------------------- report


def _collect(conf, src_host, src_ct, dst_host, dst_ct, key):
    dst_dir = "%s/%s" % (conf.agg, key)
    bash_in(dst_host, dst_ct, "mkdir -p %s" % dst_dir)
    src = subprocess.Popen(
        _docker_argv(src_host, src_ct, False)
        + ["tar", "-czf", "-", "-C", conf.work, "logs"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    dst = subprocess.Popen(
        _docker_argv(dst_host, dst_ct, True)
        + ["tar", "-xzf", "-", "-C", dst_dir, "--strip-components=1"],
        stdin=src.stdout, stderr=subprocess.PIPE,
    )
    src.stdout.close()
    _out, err = dst.communicate()
    src.wait()
    if dst.returncode != 0:
        sys.stderr.write(err.decode(errors="replace"))
        die("log collection failed from %s/%s" % (src_host, src_ct))


def report(conf):
    m = conf.row("master")
    bash_in(
        m.host, m.container,
        "mkdir -p %s && rm -rf %s/* && cp %s/run.env %s/run.env && cp %s/conf/nodes.conf %s/nodes.conf && echo %d > %s/nseg"
        % (conf.agg, conf.agg, conf.work, conf.agg, conf.work, conf.agg, conf.nseg, conf.agg),
    )
    for host, ct in conf.pairs():
        note("collecting logs from %s@%s" % (ct, host))
        # scripts only: never overwrite the run's own run.env / nodes.conf
        payload = build_payload(conf, "", with_conf=False, with_env=False)
        push(host, ct, payload, conf.work, wipe=False)
        _collect(conf, host, ct, m.host, m.container, "%s-%s" % (host, ct))

    script = (
        "python3 {w}/report.py --run-dir {a} --nseg {n} --run-id {rid} "
        "--segment-size {ss} --master-host {mh} --writer-host {wh} --out {a}/report.md && "
        "python3 {w}/report.py --run-dir {a} --nseg {n} --run-id {rid} "
        "--segment-size {ss} --master-host {mh} --writer-host {wh} --lang zh "
        "--out {a}/report.zh.md"
    ).format(w=conf.work, a=conf.agg, n=conf.nseg, rid=conf.run_id,
             ss=default_size(), mh=m.host, wh=conf.row("writer").host)
    bash_in(m.host, m.container, script)

    _rc, out, _err = bash_in(m.host, m.container, "[ -d /workspace ] && echo yes || echo no", check=False)
    dest = "%s/%s" % (RUNS_ROOT, conf.run_id)
    if out.strip().endswith("yes"):
        chown = ("&& chown -R %s %s" % (ARTIFACT_OWNER, dest)) if ARTIFACT_OWNER else ""
        rc, _o, err = bash_in(
            m.host, m.container,
            "mkdir -p {d} && cp {a}/report.md {a}/report.zh.md {a}/run.env {a}/nodes.conf {a}/nseg {d}/ {c}".format(
                d=dest, a=conf.agg, c=chown),
            check=False,
        )
        if rc == 0:
            print("artifacts: %s@%s:%s/report.md" % (m.container, m.host, dest))
            return
        tail = (err or "").strip().splitlines()
        sys.stderr.write("[ks] WARNING: archiving to %s failed (%s); the report stays in %s\n"
                         % (dest, tail[-1] if tail else "rc=%d, no stderr" % rc, conf.agg))
    print("artifacts: %s@%s:%s/report.md" % (m.container, m.host, conf.agg))


# ---------------------------------------------------------------- commands


def run_all(conf, rounds):
    try:
        start(conf, rounds)
        wait_fill(conf, FILL_TIMEOUT)
        watch_evictions(conf, rounds)
        report(conf)
    finally:
        stop(conf)


def smoke(conf):
    global SIZE_OVERRIDE, WORKERS, BATCH_SIZE, SAMPLE_INTERVAL
    SIZE_OVERRIDE, WORKERS, BATCH_SIZE, SAMPLE_INTERVAL = "1GiB", "8", "1024", "5"
    note("smoke: %d segments x 1GiB" % conf.nseg)
    try:
        start(conf)
        time.sleep(60)
        status(conf)
        report(conf)
    finally:
        stop(conf)


USAGE = """Usage: ks_control.py <command> <conf> [args]

Where it runs: this script runs in whatever shell you launch it from. Use
--driver to control what it may assume about that shell:

  --driver master  (default) must run on the master host; aborts elsewhere
  --driver host    must run on any conf node
  --driver auto    run anywhere; use `docker exec` for the node you are on
  --driver local   run anywhere; always ssh (typical on a laptop)

<conf> is the topology file. It can be a path, a bare name inside conf/, or
just the build name:

  python3 ks_control.py check    conf/nodes.6seg.conf
  python3 ks_control.py run       nodes.6seg.conf 8
  python3 ks_control.py smoke     6seg
  python3 ks_control.py report    nodes.4seg.conf 20261123-112522-4seg

The segment count is read from the conf: holder rows + 1 (the writer owns one
segment). You never pass it on the command line.

Commands:
  where           print master host and the exact command to run there
  check           packages installed? versions identical across nodes?
  plan            capacity math vs real node memory
  preflight       per-node checks (packages, RDMA, ports) + version consistency
  start           master -> sampler -> holders -> writer
  watch [N]       wait for fill to cap, then watch N eviction rounds
  status          current metrics, eviction lines, writer timeouts, memory
  report [id]     render report.md and report.zh.md (id defaults to the latest run)
  stop            writer -> holders -> master
  run [N]         start + watch + report + stop
  smoke           1GiB wiring check

conf format (4-6 columns, one row per role):
  role  ssh_host  container  local_hostname  [devices]  [segment_size]

  devices may be omitted (or '-') to let the engine auto-discover every usable
  HCA on that node; list them (comma separated) only when you want to pin a
  segment to specific cards.
  segment_size is per row, e.g. 64GiB for a holder and 32GiB for another.
  Rows without it use SEGMENT_SIZE. A 5th column that looks like a size is
  read as segment_size; otherwise it is read as devices.

Environment:
  SEGMENT_SIZE=40GiB VALUE_SIZE=64 BATCH_SIZE=12288 WORKERS=128
  EVICT_ROUNDS=8 SAMPLE_INTERVAL=15 FILL_TIMEOUT=7200 ROUND_TIMEOUT=1800
  EVICTION_RATIO=0.1 EVICTION_HIGH_WATERMARK=0.9 ALLOCATION_STRATEGY=random
  MASTER_PORT=50551 METADATA_PORT=38080 METRICS_PORT=39003
  RUN_ID=<id> CONF_OVERRIDE=<path>
  RUNTIME_DIR=-       (default) run the package installed in each container
  RUNTIME_DIR=<dir>   run a staged tree instead (must be identical on every node)
  ARTIFACT_OWNER=<uid:gid> (optional)
  RUNS_ROOT=/workspace/mooncake-realkey/keyscale-runs (in master container)

Run this anywhere that can ssh to every node in the conf. The conf, run.env and
scripts are pushed into each container's /tmp/ks/<run_id>; logs are pulled back
into the master container at report time. No host files, no shared filesystem.
"""


def main(argv):
    global DRIVER_MODE
    cleaned, i = [], 0
    while i < len(argv):
        a = argv[i]
        if a.startswith("--driver="):
            DRIVER_MODE = a.split("=", 1)[1]
            i += 1
            continue
        if a == "--driver" and i + 1 < len(argv):
            DRIVER_MODE = argv[i + 1]
            i += 2
            continue
        cleaned.append(a)
        i += 1
    argv = cleaned
    if DRIVER_MODE not in ("master", "host", "local", "auto"):
        die("bad --driver value: %s (master|host|local|auto)" % DRIVER_MODE)

    if not argv or argv[0] in ("-h", "--help"):
        sys.stderr.write(USAGE)
        return 1
    cmd = argv[0]
    known = ("where", "check", "plan", "preflight", "start", "watch", "status",
             "report", "stop", "run", "smoke")
    if cmd not in known:
        sys.stderr.write(USAGE)
        return 1

    target = argv[1] if len(argv) > 1 else ""
    rest = argv[2:]
    if not target:
        sys.stderr.write(USAGE)
        return 1

    conf = load_conf(resolve_conf(target))

    me = {socket.gethostname(), socket.gethostname().split(".")[0]}
    if cmd != "where":
        if DRIVER_MODE == "master":
            mh = conf.row("master").host
            if mh not in me:
                die("driver mode 'master' means: run this ON the master host (%s), not here (%s).\n"
                    "  ssh %s \"python3 %s %s %s\""
                    % (mh, socket.gethostname(), mh, os.path.abspath(__file__), cmd, conf.short_name()))
        elif DRIVER_MODE == "host":
            if not (me & set(conf.hosts())):
                die("--driver host requires running on a conf node; this machine is %s, conf has %s"
                    % (socket.gethostname(), ", ".join(conf.hosts())))

    run_id = os.environ.get("RUN_ID", "")
    if cmd == "report" and rest:
        run_id = rest[0]
        rest = rest[1:]
    if not run_id:
        run_id = "%s-%dseg" % (time.strftime("%Y%m%d-%H%M%S"), conf.nseg)
    conf.run_id = run_id
    conf.work = "/tmp/ks/%s" % run_id
    conf.agg = "%s/agg" % conf.work

    if cmd == "where":
        where(conf)
        return 0
    if cmd == "check":
        return check(conf)
    if cmd == "plan":
        plan(conf)
        return 0
    if cmd == "preflight":
        preflight(conf)
        return 0
    if cmd == "start":
        start(conf)
        return 0
    if cmd == "watch":
        rounds = int(rest[0]) if rest else EVICT_ROUNDS
        wait_fill(conf, FILL_TIMEOUT)
        watch_evictions(conf, rounds)
        record_rounds(conf, rounds)
        status(conf)
        return 0
    if cmd == "status":
        status(conf)
        return 0
    if cmd == "report":
        report(conf)
        return 0
    if cmd == "stop":
        stop(conf)
        return 0
    if cmd == "run":
        rounds = int(rest[0]) if rest else EVICT_ROUNDS
        run_all(conf, rounds)
        return 0
    if cmd == "smoke":
        smoke(conf)
        return 0
    sys.stderr.write(USAGE)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
