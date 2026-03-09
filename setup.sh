#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

VENV_DIR="${VENV_DIR:-$SCRIPT_DIR/.venv}"
INDEX_URL="${1:-${CORP_PIP_INDEX_URL:-${PIP_INDEX_URL:-}}}"
TOML_DIR="$SCRIPT_DIR/toml_files"
OUTPUT_DIR="$SCRIPT_DIR/find_source_excel"

choose_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    if "$PYTHON_BIN" -c "import sys" >/dev/null 2>&1; then
      PYTHON_CMD=("$PYTHON_BIN")
      return
    fi
    echo "ERROR: PYTHON_BIN is set but does not work: $PYTHON_BIN" >&2
    exit 1
  fi

  if command -v python3 >/dev/null 2>&1 && python3 -c "import sys" >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
    return
  fi

  if command -v python >/dev/null 2>&1 && python -c "import sys" >/dev/null 2>&1; then
    PYTHON_CMD=(python)
    return
  fi

  if command -v py >/dev/null 2>&1 && py -3 -c "import sys" >/dev/null 2>&1; then
    PYTHON_CMD=(py -3)
    return
  fi

  echo "ERROR: No working Python interpreter found." >&2
  echo "Install Python 3 or set PYTHON_BIN in .env, for example:" >&2
  echo "PYTHON_BIN=/c/Users/<user>/AppData/Local/Programs/Python/Python312/python.exe" >&2
  exit 1
}

resolve_venv_python() {
  if [[ -x "$VENV_DIR/bin/python" ]]; then
    VENV_PYTHON="$VENV_DIR/bin/python"
    ACTIVATE_HINT="source '$VENV_DIR/bin/activate'"
    return
  fi

  if [[ -x "$VENV_DIR/Scripts/python.exe" ]]; then
    VENV_PYTHON="$VENV_DIR/Scripts/python.exe"
    ACTIVATE_HINT="source '$VENV_DIR/Scripts/activate'"
    return
  fi

  echo "ERROR: Can't find python inside virtual environment: $VENV_DIR" >&2
  exit 1
}

choose_python
"${PYTHON_CMD[@]}" -m venv "$VENV_DIR"
resolve_venv_python

mkdir -p "$TOML_DIR" "$OUTPUT_DIR"

"$VENV_PYTHON" -m pip install --upgrade pip

echo "Trying to install dependencies from default PyPI..."
if "$VENV_PYTHON" -m pip install -r "$SCRIPT_DIR/requirements.txt"; then
  echo "Dependencies installed from default PyPI."
else
  if [[ -z "$INDEX_URL" ]]; then
    echo "Default PyPI install failed and CORP_PIP_INDEX_URL is not set."
    echo "Fill CORP_PIP_INDEX_URL in .env or pass URL as first argument."
    echo "Usage: ./setup.sh https://your.corp/simple"
    exit 1
  fi

  echo "Default PyPI install failed. Retrying with corporate index..."
  "$VENV_PYTHON" -m pip install --index-url "$INDEX_URL" -r "$SCRIPT_DIR/requirements.txt"
fi

echo "Environment is ready."
echo "Created folders: $TOML_DIR and $OUTPUT_DIR"
echo "Put TOML files into: $TOML_DIR"
echo "Excel output will be created in: $OUTPUT_DIR"
echo "Activate it with: $ACTIVATE_HINT"