#!/usr/bin/env bash
# diagnose_before_build.sh — run inside cibuildwheel's manylinux container to
# print environment info and clearly identify which requirement fails to install.
# USAGE (from cibuildwheel):
#   CIBW_BEFORE_BUILD_LINUX: bash {package}/dev/diagnose_before_build.sh "{package}"

set -Eeuo pipefail

# --- Inputs & basic sanity checks ------------------------------------------------
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <PACKAGE_DIR>   # pass {package} from cibuildwheel"
  exit 2
fi

PKG_DIR="$1"                         # e.g., /project/flink-python
REQ_FILE="${PKG_DIR}/dev/dev-requirements.txt"

echo "=== Paths ==="
echo "CWD:       $(pwd)"
echo "PKG_DIR:   ${PKG_DIR}"
echo "REQ_FILE:  ${REQ_FILE}"
echo

if [[ ! -d "${PKG_DIR}" ]]; then
  echo "ERROR: Package dir does not exist: ${PKG_DIR}"
  echo "Listing /project for debugging:"
  ls -la /project || true
  exit 2
fi

if [[ ! -f "${REQ_FILE}" ]]; then
  echo "ERROR: Requirements file not found at ${REQ_FILE}"
  echo "Try to locate it under ${PKG_DIR}:"
  ( set +e; find "${PKG_DIR}" -maxdepth 3 -name "dev-requirements.txt" -print || true )
  echo "Directory listing of ${PKG_DIR}/dev:"
  ls -la "${PKG_DIR}/dev" || true
  exit 2
fi

# --- System / Python / Pip info --------------------------------------------------
echo "=== System info (inside cibuildwheel container) ==="
uname -a || true
cat /etc/os-release || true
echo

echo "=== Python info ==="
python - <<'PY'
import sys, platform, sysconfig
print("executable:", sys.executable)
print("version:", sys.version.replace("\n"," "))
print("implementation:", sys.implementation.name)
print("platform:", platform.platform())
print("py_platform:", sysconfig.get_platform())
PY
echo

echo "=== Pip info ==="
python -m pip --version || true
# pip debug may be noisy / change format; don't fail the script if it errors.
python -m pip debug --verbose || true
echo

# --- Install requirements with maximal diagnostics --------------------------------
echo "=== Installing build/dev requirements ==="
echo "Using requirements file: ${REQ_FILE}"
# Fresh log locations (overwritten each run)
PIP_LOG=/tmp/pip_install.log
PIP_REPORT=/tmp/pip_report.json

python -m pip install -vvv --report "${PIP_REPORT}" --log "${PIP_LOG}" -r "${REQ_FILE}" || {
  echo
  echo "!!! pip install failed. Summarizing likely culprit(s) from ${PIP_LOG}:"
  # Grep for frequent failure signatures.
  ( grep -nE \
      "ERROR:|No matching distribution found for |Could not find a version that satisfies the requirement |Failed building wheel for |did not run successfully|subprocess-exited-with-error|Build failed" \
      "${PIP_LOG}" || true )
  echo

  echo "=== Heuristics: most likely failing requirement ==="
  # Try to extract probable package names from common pip error lines.
  awk '
  /^ERROR: Could not find a version that satisfies the requirement /{
    for (i=10;i<=NF;i++) printf "%s%s", $i, (i<NF?" ":"\n")
  }
  /^ERROR: No matching distribution found for /{
    print $NF
  }
  /^Failed building wheel for /{
    print $NF
  }
  /Building wheel for .* \(pyproject.toml\) did not run successfully/{
    gsub(/.*Building wheel for /,""); gsub(/ \(pyproject.toml\) did not run successfully.*/,""); print
  }' "${PIP_LOG}" | sed "s/[()':,]//g" | sort -u || true
  echo

  echo "=== Tail of pip log (last 200 lines) ==="
  tail -n 200 "${PIP_LOG}" || true
  echo

  echo "=== Parsed failures from pip JSON report (if any) ==="
  python - "${PIP_REPORT}" <<'PY'
import json, sys, pathlib
p = pathlib.Path(sys.argv[1])
if not p.exists():
    print("No pip report produced.")
    sys.exit(0)
data = json.loads(p.read_text())
fails = [i for i in data.get("install", []) if i.get("error")]
if not fails:
    print("No structured error entries found in report.")
else:
    print(f"Failed items ({len(fails)}):")
    for f in fails:
        meta = f.get("metadata", {})
        name = meta.get("name") or f.get("download_info", {}).get("url") or "unknown"
        print("-", name, "->", f.get("error"))
PY
  echo

  echo "=== pip index versions (diagnostic; shows what wheels exist for this interpreter) ==="
  for P in pemja apache-beam pyarrow numpy grpcio; do
    echo "--- $P ---"
    # Do not fail if pip index is rate-limited or not available.
    python -m pip index versions "$P" 2>&1 | tail -n +1 || true
  done
  echo

  echo "=== Quick import probe of common dev deps ==="
  python - <<'PY'
mods = ["pemja", "apache_beam", "pyarrow", "numpy", "grpc"]
import importlib
for m in mods:
    try:
        importlib.import_module(m)
        print(f"OK: import {m}")
    except Exception as e:
        print(f"FAIL: import {m} -> {e.__class__.__name__}: {e}")
PY
  echo

  echo "=== Environment snapshot ==="
  python - <<'PY'
import os
for k in sorted(os.environ):
    if any(s in k.lower() for s in ("token","secret","passwd","password","key")):
        continue
    print(f"{k}={os.environ[k]}")
PY
  exit 1
}

echo
echo "=== Requirements installed successfully ==="
