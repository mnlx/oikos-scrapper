#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if command -v python3.13 >/dev/null 2>&1; then
  PYTHON_BIN="python3.13"
elif command -v python3.12 >/dev/null 2>&1; then
  PYTHON_BIN="python3.12"
else
  echo "python3.12 or python3.13 is required for local dbt support." >&2
  echo "Use the workflow image or Dockerfile if only python3.14 is available." >&2
  exit 1
fi

rm -rf .venv
"$PYTHON_BIN" -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
python -m playwright install chromium

echo "Local environment ready in $ROOT_DIR/.venv"
