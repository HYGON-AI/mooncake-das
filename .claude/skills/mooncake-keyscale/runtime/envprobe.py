import importlib, importlib.metadata as md, os, re, shutil, subprocess, sys

def run(cmd, timeout=30):
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (p.stdout or "").strip()
    except Exception:
        return ""

host = run("hostname") or "?"
py = run("python3 -c 'import platform;print(platform.python_version())'") or "?"

# 1) what is actually installed (site-packages), independent of sys.path tricks.
#    This is the version `check` compares across nodes: the environment must be
#    identical everywhere, so we report the installed distribution, not a staged copy.
pkgs = []
try:
    for d in md.distributions():
        try:
            name = (d.metadata.get("Name") or "")
        except Exception:
            continue
        if "mooncake" in name.lower():
            pkgs.append("%s==%s" % (name, d.version))
except Exception:
    pass
pkgver = ",".join(sorted(dict.fromkeys(pkgs))) or "-"

# 2) what would actually be imported at run time. RUNTIME_DIR="-"/empty means
#    "use the installed package"; a dir means "stage that dir on sys.path first".
rt = os.environ.get("RUNTIME_DIR", "")
staged = bool(rt) and rt != "-"
if staged and os.path.isdir(os.path.join(rt, "mooncake")):
    sys.path.insert(0, rt)
    rtstate = "staged"
elif staged:
    rtstate = "missing"
else:
    rtstate = "off"
try:
    import mooncake
    import mooncake.version as v
    origin_dir = os.path.dirname(os.path.abspath(mooncake.__file__))
    modver = getattr(v, "__hcu_version__", None) or getattr(v, "__version__", "?") or "?"
    origin = "staged" if staged and origin_dir.startswith(os.path.abspath(rt)) else "installed"
except Exception as e:
    origin_dir = ""
    modver = "?"
    origin = "MISSING"

# 3) master binary: MASTER_BIN > import dir > RUNTIME_DIR > PATH
cands = [os.environ.get("MASTER_BIN", "")]
if origin_dir:
    cands.append(os.path.join(origin_dir, "mooncake_master"))
if staged:
    cands.append(os.path.join(rt, "mooncake", "mooncake_master"))
master = ""
for cand in cands:
    if cand and os.path.isfile(cand) and os.access(cand, os.X_OK):
        master = cand
        break
if not master:
    master = shutil.which("mooncake_master") or ""

git = "-"
if master:
    line = run("'%s' --version" % master)
    m = re.search(r"git:\s*([0-9a-fA-F]+)", line)
    if m:
        git = m.group(1)
    else:
        m = re.search(r"version\s+([0-9][^\s(]*)", line)
        if m:
            git = "v" + m.group(1)

devs = run("ibv_devices 2>/dev/null").splitlines()
hca = len([l for l in devs[2:] if l.strip()])
ulim = run("bash -lc 'ulimit -l' 2>/dev/null") or run("ulimit -l") or "?"

print("|".join([host, py, pkgver, modver, origin, git, str(hca), ulim, master or "-", rtstate]))
