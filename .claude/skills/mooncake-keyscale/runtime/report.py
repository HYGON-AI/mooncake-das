#!/usr/bin/env python3
"""Render the reference report for one key-scale run.

Inputs are whatever the run actually produced, all under the run dir:
  <run>/nodes.conf              node + segment inventory used
  <run>/run.env                 knobs mc4.sh ran with
  <run>/<node>/master.log       [EVICT-TRIGGER] / [EVICT-DONE] lines
  <run>/<node>/sampler.csv      key_count / clients / master RSS timeline
  <run>/<node>/loader.log       writer progress + result_codes

Nothing here invents numbers: a missing input shows up as n/a so a partial
run is still readable.

Statistics are clipped to the first `EVICT_ROUNDS` eviction rounds (recorded in
run.env by `run <conf> [rounds]`, default 8): samples and progress lines after
that window (leftover processes of a run that was never stopped, or the next run
reusing the same ports) are ignored.
"""
import argparse
import csv
import json
import os
import re
import unicodedata

try:
    import i18n as _i18n
except ImportError:  # localization is optional
    _i18n = None

LANG = "en"


def is_zh():
    return LANG == "zh"

ROUNDS = 8  # fallback when run.env has no EVICT_ROUNDS
MAX_KEYS_PER_SEG = 64 * 1024 * 1024
UNKNOWN = "n/a"

EVICT_RE = re.compile(r"\[(EVICT-TRIGGER|EVICT-DONE|EVICT-RESULT)\]")
TS_RE = re.compile(r"(\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2})")
# glog style: I1117 23:45:01.123456
GLOG_RE = re.compile(r"^[IWEF](\d{2})(\d{2})\s+(\d{2}:\d{2}:\d{2})")
GLOG_FULL_RE = re.compile(
    r"^[IWEF](\d{2})(\d{2})\s+(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?")
ISO_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})")
# "64GiB" / "1GB" / "40" -> a segment size, not a device list
SIZE_TOKEN_RE = re.compile(r"^\d+(?:\.\d+)?\s*[A-Za-z]{0,3}$")

BANNER_RES = {
    "version": re.compile(r"Mooncake master version:\s*(\S+)\s*\(git:\s*([0-9a-fA-F]+)"),
    "rpc_threads": re.compile(r"rpc_thread_num=(\d+)"),
    "discard_timeout": re.compile(r"put_start_discard_timeout_sec=(\d+)"),
    "client_ttl": re.compile(r"client_active_ttl_sec=(\d+)"),
    "evict_ratio": re.compile(r"eviction_ratio=([0-9.]+)"),
    "high_wm": re.compile(r"eviction_high_watermark_ratio=([0-9.]+)"),
    "enable_ha": re.compile(r"enable_ha=(\d)"),
}


# ---------------------------------------------------------------- inputs


def read_env(path):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def read_nodes(path):
    """rows: [role, ssh_host, local_hostname, devices, segment_size]

    Columns 5 and 6 are optional. A 5th column that looks like a size is the
    segment size, otherwise it is the device list (same rule as the driver).
    """
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 4:
                continue
            role, host, _container, ip = parts[:4]
            devices, size = "", ""
            if len(parts) >= 6:
                devices, size = parts[4], parts[5]
            elif len(parts) == 5:
                if SIZE_TOKEN_RE.match(parts[4]):
                    size = parts[4]
                else:
                    devices = parts[4]
            if devices == "-":
                devices = ""
            if size == "-":
                size = ""
            rows.append([role, host, ip, devices, size])
    return rows


def read_sampler(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            rows.append(row)
    return rows


def as_int(value, default=-1):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def as_float(value, default=-1.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def evict_events(master_log):
    """Return [(kind, raw_timestamp, line)] in file order."""
    events = []
    if not os.path.exists(master_log):
        return events
    with open(master_log, errors="replace") as fh:
        for line in fh:
            m = EVICT_RE.search(line)
            if not m:
                continue
            ts = ""
            t = TS_RE.search(line)
            if t:
                ts = t.group(1)
            else:
                g = GLOG_RE.match(line)
                if g:
                    ts = f"{g.group(1)}-{g.group(2)} {g.group(3)}"
            events.append((m.group(1), ts, line.rstrip()))
    return events


def loader_summary(loader_log):
    """Progress records, raw timeout lines and the first failed key.

    The writer emits cumulative progress snapshots, so the run total is the
    last record; only a log that emits per-interval counters is summed.
    Timeout lines are kept raw so the report can say when they happened.
    """
    records = []
    timeout_lines = []
    first_fail_key = None
    if not os.path.exists(loader_log):
        return records, timeout_lines, first_fail_key
    dec = json.JSONDecoder()
    with open(loader_log, errors="replace") as fh:
        for line in fh:
            if "Connection timed out" in line:
                timeout_lines.append(line.rstrip())
            if first_fail_key is None and "Operation for key " in line:
                m = re.search(r"Operation for key (\S+) failed", line)
                if m:
                    first_fail_key = m.group(1)
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                rec, _end = dec.raw_decode(line)
            except ValueError:
                continue
            if isinstance(rec, dict) and rec.get("event") == "progress":
                records.append(rec)
    return records, timeout_lines, first_fail_key


def loader_totals(records):
    """(last_record, codes) for a list of progress records."""
    if not records:
        return None, {}
    attempted = [as_int(r.get("attempted"), 0) for r in records]
    cumulative = all(b >= a for a, b in zip(attempted, attempted[1:]))
    if cumulative:
        last = records[-1]
        codes = {}
        for k, v in (last.get("result_codes") or {}).items():
            codes[str(k)] = as_int(v, 0)
        return last, codes
    codes = {}
    for rec in records:
        for k, v in (rec.get("result_codes") or {}).items():
            codes[str(k)] = codes.get(str(k), 0) + as_int(v, 0)
    return records[-1], codes


def find_node_file(run_dir, name):
    """Locate <run>/<node>/<name>; returns (path, node) or (None, None)."""
    for entry in sorted(os.listdir(run_dir)):
        d = os.path.join(run_dir, entry)
        if not os.path.isdir(d):
            continue
        p = os.path.join(d, name)
        if os.path.exists(p):
            return p, entry
    return None, None


def master_banner(master_log):
    """Version / RPC threading / eviction knobs from the master startup banner."""
    info = {}
    if not master_log or not os.path.exists(master_log):
        return info
    with open(master_log, errors="replace") as fh:
        for i, line in enumerate(fh):
            if i > 60:
                break
            m = BANNER_RES["version"].search(line)
            if m:
                info["version"] = m.group(1)
                info["git"] = m.group(2)
            for key in ("rpc_threads", "discard_timeout", "client_ttl",
                        "evict_ratio", "high_wm", "enable_ha"):
                m = BANNER_RES[key].search(line)
                if m and key not in info:
                    info[key] = m.group(1)
    return info


# ---------------------------------------------------------------- formatting


def fmt(n):
    if isinstance(n, int) and n >= 0:
        return f"{n:,}"
    return UNKNOWN


def human_count(n):
    if not isinstance(n, int) or n < 0:
        return UNKNOWN
    if is_zh():
        if n >= 10 ** 8:
            return "%.2f 亿" % (n / 10.0 ** 8)
        if n >= 10 ** 4:
            return "%.1f 万" % (n / 10.0 ** 4)
        return f"{n:,}"
    if n >= 10 ** 9:
        return "%.2fB" % (n / 10.0 ** 9)
    if n >= 10 ** 6:
        return "%.1fM" % (n / 10.0 ** 6)
    return f"{n:,}"


def disp_width(text):
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
               for ch in text)


def pad(text, width):
    return text + " " * max(0, width - disp_width(text))


def fmt_dur(seconds):
    if seconds is None or seconds < 0:
        return UNKNOWN
    if is_zh():
        if seconds < 60:
            return "%.1f 秒" % seconds
        m, s = divmod(int(round(seconds)), 60)
        if m < 60:
            return "%d 分 %d 秒" % (m, s)
        h, m = divmod(m, 60)
        return "%d 时 %d 分" % (h, m)
    if seconds < 60:
        return "%.1fs" % seconds
    m, s = divmod(int(round(seconds)), 60)
    if m < 60:
        return "%dm %ds" % (m, s)
    h, m = divmod(m, 60)
    return "%dh %dm" % (h, m)


def seg_bytes(text):
    m = re.match(r"^([0-9.]+)\s*([A-Za-z]*)$", (text or "").strip())
    if not m:
        return 0
    n = float(m.group(1))
    u = (m.group(2) or "B").upper()
    mul = {"B": 1, "K": 10**3, "KB": 10**3, "KI": 1024, "KIB": 1024,
           "M": 10**6, "MB": 10**6, "MI": 1024**2, "MIB": 1024**2,
           "G": 10**9, "GB": 10**9, "GI": 1024**3, "GIB": 1024**3}.get(u, 1)
    return int(n * mul)


def keys_for_segment(text):
    n = seg_bytes(text)
    if n <= 0:
        return 0
    return min(n // 1024, MAX_KEYS_PER_SEG)


# ---------------------------------------------------------------- clocks

_MONTH_DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
# glog lines carry no year; we borrow the year of the sampler timestamps so
# February 29 and March 1 get distinct day indexes.
RUN_YEAR = None


def log_clock(line):
    """glog prefix -> (month, day, seconds_of_day) or None."""
    m = GLOG_FULL_RE.match(line)
    if not m:
        return None
    frac = float("0." + m.group(6)) if m.group(6) else 0.0
    sec = int(m.group(3)) * 3600 + int(m.group(4)) * 60 + int(m.group(5)) + frac
    return (int(m.group(1)), int(m.group(2)), sec)


def clock_text(line):
    m = GLOG_FULL_RE.match(line)
    if not m:
        return ""
    frac = (m.group(6) or "")[:3]
    txt = "%s:%s:%s" % (m.group(3), m.group(4), m.group(5))
    return txt + ("." + frac if frac else "")


def clock_abs(clock):
    """Monotonic seconds inside a run, so month boundaries still sort."""
    if not clock:
        return None
    return _day_of_year(clock[0], clock[1]) * 86400 + clock[2]


def _day_of_year(month, day):
    """Day index inside the run's year; the month is part of the key.

    February 29 exists only when the sampler showed a leap year. Without any
    sampler data the year is unknown and a non-leap calendar is assumed.
    Only a run that straddles New Year would wrap.
    """
    if not 1 <= month <= 12:
        return day
    days = list(_MONTH_DAYS)
    if RUN_YEAR and RUN_YEAR % 4 == 0 and (RUN_YEAR % 100 != 0 or RUN_YEAR % 400 == 0):
        days[1] = 29
    return sum(days[:month - 1]) + day


def iso_clock(value):
    """'2026-11-23T15:05:07+0800' -> (month, day, seconds_of_day) or None."""
    m = ISO_RE.search(value or "")
    if not m:
        return None
    sec = int(m.group(4)) * 3600 + int(m.group(5)) * 60 + int(m.group(6))
    return (int(m.group(2)), int(m.group(3)), sec)


def iso_abs(value):
    clock = iso_clock(value)
    return clock_abs(clock) if clock else None


def hhmmss(seconds):
    if seconds is None:
        return UNKNOWN
    seconds = int(seconds) % 86400
    return "%02d:%02d:%02d" % (seconds // 3600, seconds % 3600 // 60, seconds % 60)


# ---------------------------------------------------------------- eviction


def pair_rounds(events, limit=ROUNDS):
    """Pair the i-th TRIGGER with the i-th DONE, in file order."""
    triggers = [e for e in events if e[0] == "EVICT-TRIGGER"]
    dones = [e for e in events if e[0] == "EVICT-DONE"]
    pairs = []
    for i in range(min(limit, len(triggers), len(dones))):
        tl, dl = triggers[i][2], dones[i][2]
        pairs.append((clock_text(tl), clock_abs(log_clock(tl)),
                      clock_text(dl), clock_abs(log_clock(dl))))
    return pairs


def round_delta(trig, done, points, triggers):
    """evicted_key_count delta between the samples bracketing one round."""
    if trig is None or done is None or not points or done < trig:
        return None, 0
    base = end = None
    for t, v in points:
        if t <= trig:
            base = (t, v)
        if t <= done:
            end = (t, v)
    if not base or not end or end[0] <= base[0]:
        return None, 0
    delta = end[1] - base[1]
    if delta <= 0:
        return None, 0
    span = sum(1 for b in triggers if base[0] < b <= end[0])
    return delta, span


def code_milestones(records, code):
    """(first_growth, last_growth, total) for a cumulative result code.

    first_growth = (abs_time, succeeded_at_that_point, cumulative_value)
    """
    prev = 0
    first = last = None
    for rec in records:
        v = as_int((rec.get("result_codes") or {}).get(code), 0)
        if v > prev:
            if first is None:
                first = (iso_abs(rec.get("time")),
                         as_int(rec.get("succeeded")), v)
            last = (iso_abs(rec.get("time")), v)
        prev = v
    return first, last, prev


def code_value_at(records, code, t_abs):
    """Cumulative value of one result code at (or before) t_abs."""
    val = 0
    if t_abs is None:
        return val
    for rec in records:
        t = iso_abs(rec.get("time"))
        if t is not None and t <= t_abs:
            val = max(val, as_int((rec.get("result_codes") or {}).get(code), 0))
    return val


def sample_near(samples, t_abs):
    """Sampler row closest to t_abs (None when there is no timeline)."""
    best = None
    best_gap = None
    if t_abs is None:
        return None
    for row in samples:
        t = iso_abs(row.get("iso"))
        if t is None:
            continue
        gap = abs(t - t_abs)
        if best_gap is None or gap < best_gap:
            best, best_gap = row, gap
    return best


def timeout_span(lines):
    """(first_text, first_abs, last_text, last_abs) from raw timeout lines."""
    stamps = []
    for line in lines:
        clock = log_clock(line)
        if clock:
            stamps.append((clock_text(line), clock_abs(clock)))
    if not stamps:
        return None, None, None, None
    return stamps[0][0], stamps[0][1], stamps[-1][0], stamps[-1][1]


def role_container(path, role):
    """Container column for one role ('' when absent or '-')."""
    if not os.path.exists(path):
        return ""
    with open(path) as fh:
        for line in fh:
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 3 and parts[0] == role and parts[2] != "-":
                return parts[2]
    return ""


# ---------------------------------------------------------------- report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--nseg", required=True)
    ap.add_argument("--segment-size", default="")
    ap.add_argument("--master-host", default="")
    ap.add_argument("--writer-host", default="")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--out", required=True)
    ap.add_argument("--lang", default="en", choices=("en", "zh"))
    args = ap.parse_args()
    global LANG, RUN_YEAR
    LANG = args.lang

    run = args.run_dir
    try:
        nseg = int(args.nseg)
    except (TypeError, ValueError):
        nseg = -1
    env = read_env(os.path.join(run, "run.env"))
    nodes = read_nodes(os.path.join(run, "nodes.conf"))
    # The driver only passes its own default here; the run's recorded env wins.
    default_size = env.get("SEGMENT_SIZE") or args.segment_size or ""

    master_log, master_node = find_node_file(run, "master.log")
    sampler_csv, _ = find_node_file(run, "sampler.csv")
    loader_log, writer_node = find_node_file(run, "loader.log")

    banner = master_banner(master_log)
    samples = read_sampler(sampler_csv) if sampler_csv else []
    RUN_YEAR = None
    for row in samples:
        m = ISO_RE.search(row.get("iso") or "")
        if m:
            RUN_YEAR = int(m.group(1))
            break
    events = evict_events(master_log) if master_log else []
    all_records, timeout_lines, first_fail_key = (loader_summary(loader_log)
                                                 if loader_log else ([], [], None))
    timeouts = len(timeout_lines)
    fail_index = -1
    if first_fail_key:
        m = re.search(r"(\d+)$", first_fail_key)
        if m:
            fail_index = int(m.group(1))

    # ---- sizes and per-segment object ceiling
    sizes = []
    for row in nodes:
        if row[0] in ("master", "writer"):
            continue
        role = row[0]
        own = env.get("SEGMENT_SIZE_%s" % role.upper(), "") if role else ""
        if len(row) > 4 and row[4]:
            sizes.append(row[4])
        elif own:
            sizes.append(own)
        else:
            sizes.append(default_size)
    if not sizes:
        sizes = [default_size] * max(nseg, 1)

    expect_cap = sum(keys_for_segment(s) for s in sizes)
    uniq = sorted({s for s in sizes if s}, key=seg_bytes)

    # ---- eviction rounds (clipped to the rounds this run asked for)
    rounds = as_int(env.get("EVICT_ROUNDS"), ROUNDS)
    if rounds <= 0:
        rounds = ROUNDS
    pairs = pair_rounds(events, rounds)
    trig_count = sum(1 for k, _, _ in events if k == "EVICT-TRIGGER")
    done_count = sum(1 for k, _, _ in events if k == "EVICT-DONE")

    # ---- write start / window
    try:
        sample_interval = int(env.get("SAMPLE_INTERVAL", "15"))
    except ValueError:
        sample_interval = 15

    first_rec = all_records[0] if all_records else None
    write_start = None
    if first_rec:
        t0 = iso_abs(first_rec.get("time"))
        if t0 is not None:
            write_start = t0 - as_float(first_rec.get("elapsed_s"), 0.0)
    if write_start is None:
        for row in samples:
            t = iso_abs(row.get("iso"))
            if t is not None and as_int(row.get("key_count")) > 0:
                write_start = t
                break

    window_end = None
    if pairs and pairs[-1][3] is not None:
        window_end = pairs[-1][3] + sample_interval

    def in_window(t):
        if t is None:
            return True
        if write_start is not None and t < write_start:
            return False
        if window_end is not None and t > window_end:
            return False
        return True

    records = [r for r in all_records if in_window(iso_abs(r.get("time")))]
    last_progress, codes = loader_totals(records)

    # ---- where the -901 burst sits: before fill, or while evicting
    first_901, last_901, total_901 = code_milestones(records, "-901")
    fill_trigger = pairs[0][1] if pairs else None
    pre_fill_901 = (code_value_at(records, "-901", fill_trigger)
                    if fill_trigger is not None else total_901)
    evict_901 = max(total_901 - pre_fill_901, 0)
    t_first_txt, t_first_abs, t_last_txt, t_last_abs = timeout_span(timeout_lines)

    points = []
    for row in samples:
        t = iso_abs(row.get("iso"))
        v = as_int(row.get("evicted_key_count"))
        if t is not None and v >= 0:
            points.append((t, v))

    def peak(key):
        best = -1
        for row in samples:
            if not in_window(iso_abs(row.get("iso"))):
                continue
            val = as_int(row.get(key))
            if val > best:
                best = val
        return best

    peak_keys = peak("key_count")
    peak_rss = -1.0
    for row in samples:
        if not in_window(iso_abs(row.get("iso"))):
            continue
        val = as_float(row.get("master_rss_gib"))
        if val > peak_rss:
            peak_rss = val
    clients = peak("active_clients")
    # master_active_clients counts segment owners; a segment-less writer is not counted
    expect_clients = nseg
    evicted_total = None
    win_points = [p for p in points if in_window(p[0])]
    if len(win_points) >= 2:
        evicted_total = win_points[-1][1] - win_points[0][1]

    # `[EVICT-RESULT] evicted_count=N` is the exact per-round count; the
    # sampler delta is only a fallback when that line is absent.
    exact_del = []
    for kind, _ts, line in events:
        if kind == "EVICT-RESULT":
            m = re.search(r"evicted_count=(\d+)", line)
            exact_del.append(int(m.group(1)) if m else -1)
    if len(exact_del) < len(pairs) or any(v < 0 for v in exact_del[:len(pairs)]):
        exact_del = []

    round_rows = []
    triggers = [p[1] for p in pairs]
    for idx, (_trig_txt, trig, _done_txt, done) in enumerate(pairs, 1):
        dur = (done - trig) if (trig is not None and done is not None) else None
        delta, span = round_delta(trig, done, points, triggers)
        if exact_del:
            delta, span = exact_del[idx - 1], 1
        round_rows.append((idx, dur, delta, span))

    if exact_del:
        evicted_total = sum(exact_del[:len(pairs)])

    # ---- counters
    code_901 = codes.get("-901", 0)
    code_200 = codes.get("-200", 0)
    try:
        batch_size = int(env.get("BATCH_SIZE", "0"))
    except (TypeError, ValueError):
        batch_size = 0
    batches = int(round(code_901 / float(batch_size))) if batch_size > 0 else 0
    succeeded = as_int((last_progress or {}).get("succeeded"))
    failed = as_int((last_progress or {}).get("failed"))
    attempted = as_int((last_progress or {}).get("attempted"))
    qps = as_float((last_progress or {}).get("key_qps"))
    if qps <= 0 and attempted > 0:
        elapsed = as_float((last_progress or {}).get("elapsed_s"))
        if elapsed > 0:
            qps = attempted / elapsed

    dropped = False
    for row in samples:
        if not in_window(iso_abs(row.get("iso"))):
            continue
        c = as_int(row.get("active_clients"))
        if c < 0 or (clients >= 0 and c < clients):
            dropped = True
            break

    version = banner.get("version", "")
    pkg = ("Mooncake %s" % version) if version else "Mooncake version n/a"
    origin = "git %s" % banner["git"] if banner.get("git") else "git n/a"
    if env.get("PROFILE"):
        origin += "; PROFILE=%s" % env["PROFILE"]

    rpc_threads = banner.get("rpc_threads", UNKNOWN)
    discard = banner.get("discard_timeout", UNKNOWN)
    high_wm = banner.get("high_wm") or "0.9"
    evict_ratio = banner.get("evict_ratio") or "0.1"
    for _kind, _ts, line in events:
        m = re.search(r"high_watermark=([0-9.]+)", line)
        if m:
            high_wm = m.group(1)
        m = re.search(r"eviction_ratio=([0-9.]+)", line)
        if m:
            evict_ratio = m.group(1)
    ha = "HA disabled" if banner.get("enable_ha") in (None, "0") else "HA enabled"

    master_host = args.master_host or next((r[1] for r in nodes if r[0] == "master"), "")
    writer_host = args.writer_host or next((r[1] for r in nodes if r[0] == "writer"), "")
    writer_row = next((r for r in nodes if r[0] == "writer"), None)
    writer_devices = []
    if writer_row and len(writer_row) > 3 and writer_row[3] and writer_row[3] != "-":
        writer_devices = [d for d in writer_row[3].split(",") if d]
    # A writer row without a segment size is a pure client: it must not claim a
    # segment, and the global SEGMENT_SIZE must not be attributed to it.
    writer_size = (writer_row[4] if writer_row and len(writer_row) > 4 else "") or ""
    master_segments = [r for r in nodes
                       if r[0] == "master" and len(r) > 4 and r[4]]

    value_size = as_int(env.get("VALUE_SIZE"))
    byte_pct = ("%.2f%%" % (value_size / 1024.0 * 100)) if value_size > 0 else "6.25%"

    L = []
    a = L.append
    a("# %s %s-segment key-capacity test" % (pkg, "N" if nseg < 0 else nseg))
    a("")
    a("Package: %s (%s). Raw samples and log summaries live in `%s`." % (pkg, origin, run))
    a("Same write pressure as always: push resident keys to the per-segment object cap, "
      "then watch %d eviction %s and see whether BatchPutStart exceeds 30 seconds "
      "before fill-up or during eviction."
      % (max(len(pairs), 0), "round" if len(pairs) == 1 else "rounds"))
    for size in uniq:
        a("%s per segment caps at about %s keys by `bytes/1024` (see the metrics table for the measured cap)."
          % (size, fmt(keys_for_segment(size))))
    if not uniq:
        a("Segment size: %s." % UNKNOWN)
    a("")

    if code_901 == 0:
        burst_txt = "no -901"
    elif pre_fill_901 > 0 and evict_901 == 0:
        burst_txt = ("one batch of -901 before fill-up (%d batches x %s = %s), none added during eviction"
                     % (batches, fmt(batch_size), fmt(pre_fill_901)))
    elif pre_fill_901 == 0:
        burst_txt = ("-901 appeared during eviction (%d batches x %s = %s)"
                     % (batches, fmt(batch_size), fmt(evict_901)))
    else:
        burst_txt = ("-901 %s before fill-up, %s more during eviction"
                     % (fmt(pre_fill_901), fmt(evict_901)))
    if timeouts:
        burst_txt += ", %d Connection timed out lines" % timeouts
    else:
        burst_txt += ", no Connection timed out"
    highlight = (("%d eviction round completed" if len(pairs) == 1 else "%d eviction rounds completed")
                 % len(pairs)) if pairs else "no complete eviction round"
    highlight += "; " + burst_txt
    if is_zh():
        if code_901 == 0:
            zh_hl = "无 -901"
        elif pre_fill_901 > 0 and evict_901 == 0:
            zh_hl = ("写满前有一批 -901（%d 批 × %s = %s），驱逐期间未再增加"
                     % (batches, fmt(batch_size), fmt(pre_fill_901)))
        elif pre_fill_901 == 0:
            zh_hl = ("驱逐期间出现 -901（%d 批 × %s = %s）"
                     % (batches, fmt(batch_size), fmt(evict_901)))
        else:
            zh_hl = ("写满前 -901 %s，驱逐期间再增 %s"
                     % (fmt(pre_fill_901), fmt(evict_901)))
        zh_hl += ("，Connection timed out %d 行" % timeouts) if timeouts \
            else "，无 Connection timed out"
        zh_rounds = ("%d 轮驱逐完成" % len(pairs)) if pairs else "未见完整驱逐轮次"
        a("| 测试 | 常驻上限 | 结果要点 |")
        a("| --- | --- | --- |")
        a("| %d 段 | %s（约 %s） | %s；%s |"
          % (nseg, fmt(expect_cap), human_count(expect_cap), zh_rounds, zh_hl))
    else:
        a("| Test | Resident cap | Result |")
        a("| --- | --- | --- |")
        a("| %d segments | %s (~%s) | %s |"
          % (nseg, fmt(expect_cap), human_count(expect_cap), highlight))
    a("")

    a("## Test method")
    a("")
    a("Write pressure:")
    a("- one writer process, %s threads, %s keys per batch, %s-byte values"
      % (env.get("WORKERS", UNKNOWN), env.get("BATCH_SIZE", UNKNOWN),
         env.get("VALUE_SIZE", UNKNOWN)))
    a("- master with %s RPC threads; client RPC timeout 30s (longer calls are recorded as -901)" % rpc_threads)
    a("- client_ttl=%s, key lease %s" % (env.get("CLIENT_TTL", UNKNOWN),
                                        env.get("LEASE", UNKNOWN)))
    high_wm = env.get("EVICTION_HIGH_WATERMARK", high_wm)
    evict_ratio = env.get("EVICTION_RATIO", evict_ratio)
    alloc = env.get("ALLOCATION_STRATEGY", "random")
    a("- eviction high watermark %s, ratio %s, allocation strategy %s" % (high_wm, evict_ratio, alloc))
    a("- %s" % ha)
    a("")
    if master_segments:
        master_txt = ("挂了 %d 段" % len(master_segments)) if is_zh() \
            else ("mounts %d segments" % len(master_segments))
    elif nodes:
        master_txt = "不挂数据段" if is_zh() else "mounts no data segment"
    else:
        master_txt = ("挂载情况%s" % UNKNOWN) if is_zh() else ("mount state %s" % UNKNOWN)
    if writer_devices:
        dev_txt = ", ".join(writer_devices)
    else:
        dev_txt = "自动（引擎发现全部 HCA）" if is_zh() else "auto (engine-discovered HCAs)"
    if writer_size:
        if is_zh():
            a("Master 在 %s，%s。写入在 %s，使用 %s 并自己挂一段（%s）。"
              % (master_host or UNKNOWN, master_txt, writer_host or UNKNOWN, dev_txt,
                 writer_size))
        else:
            a("Master on %s, %s. Writer on %s, using %s and mounting one segment (%s)."
              % (master_host or UNKNOWN, master_txt, writer_host or UNKNOWN, dev_txt,
                 writer_size))
    elif is_zh():
        a("Master 在 %s，%s。写入在 %s，使用 %s，是纯客户端，不挂段；"
          "master_active_clients 只统计 holder。"
          % (master_host or UNKNOWN, master_txt, writer_host or UNKNOWN, dev_txt))
    else:
        a("Master on %s, %s. Writer on %s, using %s; it is a pure client with no segment, "
          "so master_active_clients only counts holders."
          % (master_host or UNKNOWN, master_txt, writer_host or UNKNOWN, dev_txt))
    a("")

    a("## Node and segment layout")
    a("")
    if nodes:
        a("| Node | Role | local_hostname | Segment size |")
        a("| --- | --- | --- | --- |")
        for row in nodes:
            role, host, lh = row[0], row[1], row[2]
            own = env.get("SEGMENT_SIZE_%s" % role.upper(), "") if role else ""
            sz = (row[4] if len(row) > 4 and row[4] else own or default_size) or "-"
            a("| %s | %s | %s | %s |"
              % (host, role, lh or "-",
                 "-" if role in ("master", "writer") else sz))
    else:
        a("nodes.conf %s." % UNKNOWN)
    a("")

    a("## Why the object cap is hit first")
    a("")
    a("%s-byte values do not fill the segment byte capacity. Segments hit the object cap first, "
      "new writes return -200 and trigger eviction; the byte watermark is about %s."
      % (env.get("VALUE_SIZE", UNKNOWN), byte_pct))
    a("")

    a("## Write and eviction")
    a("")
    gap_fill = (fill_trigger - write_start) if (fill_trigger is not None and write_start is not None) else None
    if qps > 0:
        rate_txt = ("约 %s 个 key/秒" if is_zh() else "about %s keys/s") % fmt(int(round(qps)))
    else:
        rate_txt = UNKNOWN
    writer_ct = role_container(os.path.join(run, "nodes.conf"), "writer")
    if writer_ct:
        where = ("%s（容器 %s）" if is_zh() else "%s (container %s)") % (writer_host, writer_ct)
    else:
        where = writer_host or UNKNOWN
    if is_zh():
        a("写入在 %s，%s 开始，填数速率 %s；%s 写满 %s 个 key 并触发第 1 轮驱逐，距开始约 %s。"
          % (where, hhmmss(write_start), rate_txt,
             hhmmss(fill_trigger) if fill_trigger is not None else UNKNOWN,
             fmt(peak_keys), fmt_dur(gap_fill)))
    else:
        a("Writing on %s started at %s (%s); it filled %s keys at %s and triggered the first "
          "eviction round, about %s after the start."
          % (where, hhmmss(write_start), rate_txt, fmt(peak_keys),
             hhmmss(fill_trigger) if fill_trigger is not None else UNKNOWN,
             fmt_dur(gap_fill)))
    a("")

    # ---- timeline: every row carries a time, a place and the numbers at that time
    tl = []
    if write_start is not None:
        if is_zh():
            start_txt = ("%s，开始写入（%s 线程，每批 %s 个 key，value %s 字节）"
                         % (where, env.get("WORKERS", UNKNOWN),
                            env.get("BATCH_SIZE", UNKNOWN), env.get("VALUE_SIZE", UNKNOWN)))
        else:
            start_txt = ("%s; writing starts (%s threads, %s keys per batch, %s-byte values)"
                         % (where, env.get("WORKERS", UNKNOWN),
                            env.get("BATCH_SIZE", UNKNOWN), env.get("VALUE_SIZE", UNKNOWN)))
        tl.append((write_start, hhmmss(write_start), start_txt))
    stall_at = t_first_abs if t_first_abs is not None else (
        first_901[0] if first_901 else None)
    if stall_at is not None:
        snap = sample_near(samples, stall_at)
        zh = is_zh()
        detail = [("%s 发出的 BatchPutStart 到 30 秒上限仍未返回" if zh
                   else "the BatchPutStart sent at %s had still not returned at the 30s limit")
                  % hhmmss(stall_at - 30)]
        if fail_index >= 0:
            detail.append(("失败 key 从第 %s 个起" if zh else "the first failed key is #%s")
                          % fmt(fail_index))
        elif first_901 and first_901[1] >= 0:
            detail.append(("%s 个 key 已成功写入后才失败" if zh
                           else "%s keys were written successfully before the failure")
                          % fmt(first_901[1]))
        if snap:
            kc = as_int(snap.get("key_count"))
            if kc >= 0 and expect_cap > 0:
                detail.append(("当时驻留 %s（%.1f%% 上限）" if zh
                               else "resident %s (%.1f%% of the cap)")
                              % (fmt(kc), 100.0 * kc / expect_cap))
            rss = as_float(snap.get("master_rss_gib"))
            avail = as_float(snap.get("mem_avail_gib"))
            if rss >= 0:
                detail.append(("master RSS %.1f GiB" if zh else "master RSS %.1f GiB") % rss)
            if avail >= 0:
                detail.append(("节点 available %.0f GiB" if zh
                               else "node available %.0f GiB") % avail)
        if code_901 > 0:
            head = ("首次 -901（BatchPutStart >30 秒）：" if zh
                    else "first -901 (BatchPutStart >30s): ")
        else:
            head = ("首次 Connection timed out：" if zh
                    else "first Connection timed out: ")
        tl.append((stall_at, hhmmss(stall_at), head + ("；" if zh else "; ").join(detail)))
    if (last_901 and first_901 and last_901[0] is not None
            and first_901[0] is not None and last_901[0] > first_901[0]):
        tl.append((last_901[0], hhmmss(last_901[0]),
                   ("这一批超时结束：-901 停在 %s（%d 批 × %s）" if is_zh()
                    else "this timeout batch ends: -901 stops at %s (%d batches x %s)")
                   % (fmt(last_901[1]), batches, fmt(batch_size))))
    if t_first_abs is not None:
        span = ""
        if t_last_txt and t_first_txt and t_last_txt[:8] != t_first_txt[:8]:
            span = ("，最后一次 %s" if is_zh() else ", last one at %s") % t_last_txt[:8]
        tl.append((t_first_abs, t_first_txt[:8],
                   ("loader 开始报 Connection timed out，共 %d 行%s" if is_zh()
                    else "loader starts reporting Connection timed out, %d lines%s")
                   % (timeouts, span)))
    if fill_trigger is not None:
        tl.append((fill_trigger, hhmmss(fill_trigger),
                   ("写满并触发第 1 轮驱逐（常驻 %s，字节水位约 %s）" if is_zh()
                    else "filled up and triggered the first eviction round "
                         "(resident %s, byte watermark ~%s)")
                   % (fmt(peak_keys), byte_pct)))
    if pairs:
        tl.append((pairs[-1][3], pairs[-1][2][:8],
                   ("第 %d 轮 [EVICT-DONE]" if is_zh() else "round %d [EVICT-DONE]")
                   % len(pairs)))
    peak_rss_row = min_avail_row = None
    for row in samples:
        if not in_window(iso_abs(row.get("iso"))):
            continue
        r = as_float(row.get("master_rss_gib"))
        if r >= 0 and (peak_rss_row is None
                       or r > as_float(peak_rss_row.get("master_rss_gib"))):
            peak_rss_row = row
        av = as_float(row.get("mem_avail_gib"))
        if av >= 0 and (min_avail_row is None
                        or av < as_float(min_avail_row.get("mem_avail_gib"))):
            min_avail_row = row
    if peak_rss_row is not None:
        tl.append((iso_abs(peak_rss_row.get("iso")), str(peak_rss_row.get("iso", ""))[11:19],
                   ("master RSS 峰值 %.1f GiB（%s 个 key，%.2f KiB/key）" if is_zh()
                    else "master RSS peak %.1f GiB (%s keys, %.2f KiB/key)")
                   % (as_float(peak_rss_row.get("master_rss_gib")),
                      fmt(as_int(peak_rss_row.get("key_count"))),
                      as_float(peak_rss_row.get("master_rss_gib")) * 1024 ** 3
                      / max(as_int(peak_rss_row.get("key_count")), 1) / 1024.0)))
    if min_avail_row is not None:
        tl.append((iso_abs(min_avail_row.get("iso")), str(min_avail_row.get("iso", ""))[11:19],
                   ("master 节点 available 最低 %.0f GiB（统计窗口内）" if is_zh()
                    else "master node available low of %.0f GiB (lowest in the window)")
                   % as_float(min_avail_row.get("mem_avail_gib"))))
    if tl:
        tl.sort(key=lambda row: (row[0] is None, row[0] if row[0] is not None else 0))
        a("| Time | Event |")
        a("| --- | --- |")
        for _abs, when, text in tl:
            a("| %s | %s |" % (when or UNKNOWN, text))
        a("")

    if is_zh():
        label = "八轮" if len(round_rows) == 8 else "%d 轮" % len(round_rows)
        a("%s驱逐：" % label)
    else:
        label = "1 round" if len(round_rows) == 1 else "%d rounds" % len(round_rows)
        a("%s of eviction:" % label)
    a("")
    if round_rows:
        if is_zh():
            a("| 轮次 | 耗时 | 删除的 key 数 |")
        else:
            a("| Round | Duration | Keys deleted |")
        a("| --- | --- | --- |")
        for idx, dur, delta, span in round_rows:
            if delta is None:
                cells = fmt_dur(dur), UNKNOWN
            else:
                cells = fmt_dur(dur), f"{delta:,}" + (" †" if span > 1 else "")
            if is_zh():
                a("| 第 %d 轮 | %s | %s |" % (idx, cells[0], cells[1]))
            else:
                a("| round %d | %s | %s |" % (idx, cells[0], cells[1]))
        a("")
        if any(span > 1 for _, _, _, span in round_rows):
            a("† 该行的差值覆盖了相邻轮次（采样间隔大于单轮耗时，无法按轮拆分）。"
              if is_zh() else
              "† This delta spans adjacent rounds (the sample interval is longer than one "
              "round, so it cannot be split).")
            a("")
    else:
        a("No paired `[EVICT-TRIGGER]` / `[EVICT-DONE]` in master.log; eviction was never triggered.")
        a("")

    if evicted_total is not None:
        if exact_del:
            src = ("master.log [EVICT-RESULT] 逐轮求和" if is_zh()
                   else "sum of the per-round master.log [EVICT-RESULT]")
        else:
            src = "sampler 累计值" if is_zh() else "sampler cumulative"
        a(("%s合计删除 %s 个 key（来源：%s）。" if is_zh()
           else "%s, %s keys deleted in total (source: %s).")
          % (label, fmt(evicted_total), src))
    else:
        a(("%s合计删除：%s。" if is_zh() else "%s: total deleted %s.") % (label, UNKNOWN))
    if succeeded >= 0:
        comp = ("、" if is_zh() else ", ").join(
            "%s:%s" % (k, fmt(v)) for k, v in
            sorted(codes.items(), key=lambda kv: -kv[1]) if k != "0")
        if failed == 0:
            comp = "无" if is_zh() else "none"
        a(("写入成功约 %s 次，失败约 %s 次，失败构成：%s。" if is_zh()
           else "Writer succeeded about %s times and failed about %s times; failure codes: %s.")
          % (fmt(succeeded), fmt(failed), comp or UNKNOWN))
    else:
        a(("写入成功 / 失败：%s。" if is_zh() else "Writer succeeded / failed: %s.") % UNKNOWN)
    fail_codes = {k: v for k, v in codes.items() if k != "0" and v > 0}
    if code_901 == 0 and timeouts == 0:
        if fail_codes and set(fail_codes) == {"-200"}:
            a(("没有 RPC 超时（-901）：失败全部是 -200（段对象数上限），loader 日志无 "
               "Connection timed out。" if is_zh()
               else "No RPC timeout (-901): every failure is -200 (per-segment object cap); "
                    "loader log has no Connection timed out."))
        elif fail_codes:
            a(("没有 RPC 超时（-901）：失败构成见上，loader 日志无 Connection timed out。" if is_zh()
               else "No RPC timeout (-901): see the failure codes above; loader log has no "
                    "Connection timed out."))
        else:
            a(("没有 RPC 超时（-901）：loader 日志无 Connection timed out。" if is_zh()
               else "No RPC timeout (-901): loader log has no Connection timed out."))
    elif code_901 > 0:
        a(("存在 RPC 超时（-901）：-901=%s（%d 批 × %s），Connection timed out=%d 行；"
           "其中写满前 %s，驱逐期间 %s。" if is_zh()
           else "RPC timeouts (-901) present: -901=%s (%d batches x %s), Connection timed out=%d lines; "
                "before fill-up %s, during eviction %s.")
          % (fmt(code_901), batches, fmt(batch_size), timeouts,
             fmt(pre_fill_901), fmt(evict_901)))
    else:
        span = ""
        if t_first_txt and t_last_txt:
            span = ("，%s 到 %s" if is_zh() else ", from %s to %s") % (
                t_first_txt[:8], t_last_txt[:8])
        a(("没有 RPC 超时（-901）；loader 日志有 %d 行 `Connection timed out`%s。" if is_zh()
           else "No RPC timeout (-901); the loader log has %d `Connection timed out` line(s)%s.")
          % (timeouts, span))
    if clients >= 0:
        a(("%s 个段在线%s（%d 个 holder；writer 不挂段，不计入；master_active_clients 峰值 %s）。"
           if is_zh() else
           "%s segments online %s (%d holders; the writer mounts none and is not counted; "
           "master_active_clients peak %s).")
          % (clients, ("掉线过" if dropped else "一直在") if is_zh()
             else ("with drops" if dropped else "throughout"), nseg, fmt(clients)))
    else:
        a(("数据端在线情况：%s。" if is_zh() else "Online data clients: %s.") % UNKNOWN)
    a("")

    a("## What the observations show")
    a("")
    if code_901 == 0 and timeouts == 0:
        a("- No -901 and no Connection timed out at any point.")
    elif code_901 == 0:
        if t_first_txt and t_last_txt:
            span = ("，%s 到 %s" if is_zh() else ", from %s to %s") % (
                t_first_txt[:8], t_last_txt[:8])
        else:
            span = ""
        if is_zh():
            a("- 没有 -901，但 loader 日志有 %d 行 `Connection timed out`%s；"
              "没有 -901 就没有阶段分界可拆。" % (timeouts, span))
        else:
            a("- No -901, but the loader log has %d `Connection timed out` line(s)%s; "
              "with no -901 there is no phase boundary to split them by." % (timeouts, span))
    elif pre_fill_901 > 0 and evict_901 == 0:
        a("- All -901 happened before fill-up: %s accumulated before `[EVICT-TRIGGER]` (%s), "
          "and none were added through round %d `[EVICT-DONE]` (%s)."
          % (hhmmss(fill_trigger), fmt(pre_fill_901), len(pairs),
             pairs[-1][2][:8] if pairs else UNKNOWN))
    else:
        a("- -901 kept growing during eviction (%s), so writes are contending with eviction/cleanup on locks." % fmt(evict_901))
    if clients >= 0:
        drop_txt = (("中途掉过线" if dropped else "全程在线") if is_zh()
                    else ("dropped during the run" if dropped else "online throughout"))
        a(("- master_active_clients 峰值 %d（= holder 数 %d），%s。" if is_zh()
           else "- master_active_clients peak %d (= %d holders), %s.")
          % (clients, nseg, drop_txt))
    a("")

    a("## Key metrics")
    a("")
    a("| Item | Reference | Measured |")
    a("| --- | --- | --- |")
    a("| Resident key cap | %s | %s |" % (fmt(expect_cap), fmt(peak_keys)))
    a("| master_active_clients | %s | %s |" % (expect_clients, fmt(clients)))
    expect_rss = ("~%.1f GiB (1.12KiB/key)"
                  % (expect_cap * 1120.0 / 1024 ** 3)) if expect_cap > 0 else UNKNOWN
    if peak_rss > 0:
        rss_txt = "%.1f GiB" % peak_rss
        if peak_keys > 0:
            rss_txt += " (%.2f KiB/key)" % (peak_rss * 1024 ** 3 / peak_keys / 1024.0)
    else:
        rss_txt = UNKNOWN
    a("| master RSS peak | %s | %s |" % (expect_rss, rss_txt))
    a("| BatchPutStart timeout (-901) | 0 | %s |" % fmt(code_901))
    a("| Segment object cap (-200) | appears after fill-up | %s |" % fmt(code_200))
    a("| Connection timed out | 0 | %s |" % fmt(timeouts))
    a("")

    a("## Conclusion")
    a("")
    if pairs and fill_trigger is not None:
        pct = (100.0 * peak_keys / expect_cap) if expect_cap > 0 else 0.0
        if 95.0 <= pct <= 105.0:
            cap_txt = " (%.1f%% of the cap)" % pct
        elif pct > 105.0:
            cap_txt = (" (above the %s-per-segment estimate of %s from plan; the measured per-segment cap is higher)"
                       % (uniq[0] if len(uniq) == 1 else "conf", fmt(expect_cap)))
        else:
            cap_txt = " (plan estimated a cap of %s)" % fmt(expect_cap)
        a("- Writing started at %s; it filled %s keys at %s%s, taking about %s."
          % (hhmmss(write_start), fmt(peak_keys), hhmmss(fill_trigger),
             cap_txt, fmt_dur(gap_fill)))
    elif peak_keys >= 0:
        a("- Resident keys peaked at %s (eviction never triggered)." % fmt(peak_keys))
    if pairs:
        durs = [d for _i, d, _delta, _span in round_rows if d is not None]
        if durs:
            rounds_txt = "eviction round" if len(pairs) == 1 else "eviction rounds"
            a("- All %d %s completed, %s to %s per round; %s keys deleted in total."
              % (len(pairs), rounds_txt, fmt_dur(min(durs)), fmt_dur(max(durs)),
                 fmt(evicted_total)))
        else:
            rounds_txt = "eviction round" if len(pairs) == 1 else "eviction rounds"
            a("- All %d %s completed; %s keys deleted in total."
              % (len(pairs), rounds_txt, fmt(evicted_total)))
    else:
        a("- No paired `[EVICT-TRIGGER]` / `[EVICT-DONE]` was observed; eviction never started.")
    if code_901 > 0:
        a("- -901 total %s (%d batches x %s), Connection timed out %d lines; "
          "split at the first `[EVICT-TRIGGER]` (%s): before fill-up %s, during eviction %s."
          % (fmt(code_901), batches, fmt(batch_size), timeouts,
             hhmmss(fill_trigger), fmt(pre_fill_901), fmt(evict_901)))
        if pre_fill_901 > 0 and evict_901 == 0:
            a("  - The timeouts are concentrated between %s and %s; none appeared through round %d `[EVICT-DONE]` (%s)."
              % ((t_first_txt or "")[:8] or UNKNOWN,
                 (t_last_txt or "")[:8] or UNKNOWN, len(pairs),
                 pairs[-1][2][:8] if pairs else UNKNOWN))
    elif timeouts > 0:
        a("- No -901; the loader log has %d `Connection timed out` line(s) and no start-line phase "
          "boundary to split them by." % timeouts)
    else:
        a("- No -901 and no Connection timed out at any point.")
    if code_200 > 0:
        a("- -200 total %s, all after fill-up (segment object cap exhausted)." % fmt(code_200))
    if clients >= 0:
        avail_txt = UNKNOWN
        if min_avail_row is not None:
            avail_txt = "%.0f GiB" % as_float(min_avail_row.get("mem_avail_gib"))
        drop_txt = (("中途掉过线" if dropped else "全程在线") if is_zh()
                    else ("dropped during the run" if dropped else "online throughout"))
        if is_zh():
            a("- master_active_clients 峰值 %d（= holder 数 %d），%s；master RSS 峰值 %s，"
              "节点 available 最低 %s。"
              % (clients, nseg, drop_txt,
                 ("%.1f GiB" % peak_rss) if peak_rss > 0 else UNKNOWN, avail_txt))
        else:
            a("- master_active_clients peak %d (= %d holders), %s; master RSS peak %s, "
              "node available low %s."
              % (clients, nseg, drop_txt,
                 ("%.1f GiB" % peak_rss) if peak_rss > 0 else UNKNOWN, avail_txt))
    a("")

    a("## Reproduce")
    a("")
    a("```bash")
    a("python3 driver/ks_control.py check <conf>")
    a("python3 driver/ks_control.py plan <conf>")
    a("python3 driver/ks_control.py run <conf> %d" % rounds)
    a("```")
    a("")

    a("## Data sources and calculations")
    a("")
    a("- `run.env`: WORKERS/BATCH_SIZE/VALUE_SIZE/CLIENT_TTL/LEASE/SEGMENT_SIZE and so on")
    a("- `nodes.conf`: role/IP/devices/segment size (columns 5 and 6 optional)")
    a("- Segment size precedence: `nodes.conf` column 6 -> `run.env` `SEGMENT_SIZE_<ROLE>` -> "
      "`SEGMENT_SIZE` -> command-line default")
    a("- `master.log`: `[EVICT-TRIGGER]` / `[EVICT-DONE]` plus timestamps (glog format "
      "`I1123 14:44:01.123`)")
    a("- `sampler.csv`: timeline of key_count / active_clients / master_rss / evicted_key_count / "
      "mem_available")
    a("- `loader.log`: progress JSON lines (attempted/succeeded/failed/result_codes/key_qps/time)")
    a("- Missing data is always written as `%s`; never invent numbers" % UNKNOWN)
    a("- Window: write start -> round %d EVICT-DONE (sample interval %ds); leftover samples after the window "
      "(test not stopped, or the next run reusing the same master port) are ignored" % (len(pairs), sample_interval))
    a("- Round table: durations pair the i-th `[EVICT-TRIGGER]` with the i-th `[EVICT-DONE]`; deletions "
      "come from that round's `[EVICT-RESULT] evicted_count=` line; only when that line is missing does "
      "the table fall back to the `evicted_key_count` delta between the samples around the round "
      "(`%s`, or a †-marked delta that spans adjacent rounds)" % UNKNOWN)
    a("- Total deletions sum the per-round `[EVICT-RESULT] evicted_count=` values; when those are missing "
      "it falls back to the cumulative `evicted_key_count` increase inside the window")
    a("- Phase split: the first `[EVICT-TRIGGER]` is the boundary; "
      "`-901` accumulated before it counts as before fill-up, anything added after it counts as during eviction")
    a("- RSS / available / resident keys in the timeline come from the nearest sampler sample (interval %ds)"
      % sample_interval)
    a("- `-901` batches = total `-901` / `BATCH_SIZE` (one BatchPutStart per batch)")
    if trig_count > len(pairs) or done_count > len(pairs):
        a("- master.log has %d TRIGGER and %d DONE events; this table covers the first %d rounds only"
          % (trig_count, done_count, len(pairs)))
    a("")

    with open(args.out, "w") as fh:
        text = "\n".join(L) + "\n"
        if is_zh() and _i18n is not None:
            text = _i18n.localize(text).replace("n/a", "未采集")
        fh.write(text)
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
