#!/usr/bin/env bash
# Shared helpers — must be sourced, not executed directly.
# Requires: SCRIPT_DIR to be set in the sourcing script.

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

if [[ ! -t 1 ]] || [[ -n "${NO_COLOR:-}" ]]; then
  RED='' GREEN='' YELLOW='' BLUE='' BOLD='' RESET=''
fi

# ── Logging ───────────────────────────────────────────────────────────────────
log_info()    { printf "${BLUE}[INFO]${RESET}    %s\n"    "$*"; }
log_success() { printf "${GREEN}[OK]${RESET}      %s\n"   "$*"; }
log_warn()    { printf "${YELLOW}[WARN]${RESET}    %s\n"  "$*" >&2; }
log_error()   { printf "${RED}[ERROR]${RESET}   %s\n"    "$*" >&2; }

die() {
  log_error "$*"
  exit 1
}

on_error() {
  local exit_code=$1 line=$2
  log_error "Script failed (exit $exit_code) at line $line"
}

# ── wait_for_http <url> [max_seconds] ─────────────────────────────────────────
# Polls <url> every 3 seconds until HTTP 200 is returned or <max_seconds> elapses.
# Falls back through curl → wget → python3 for portability.
wait_for_http() {
  local url=$1 max_seconds=${2:-60}
  local elapsed=0 interval=3 status

  while (( elapsed < max_seconds )); do
    status=""

    if command -v curl &>/dev/null; then
      status=$(curl -o /dev/null -s -w "%{http_code}" --connect-timeout 3 "$url" 2>/dev/null || true)

    elif command -v wget &>/dev/null; then
      status=$(wget -qO- --server-response "$url" 2>&1 \
        | awk '/^  HTTP\//{print $2}' | tail -1 || true)

    else
      status=$(python3 - "$url" <<'EOF' 2>/dev/null || echo 0
import sys, urllib.request
try:
    r = urllib.request.urlopen(sys.argv[1], timeout=3)
    print(r.status)
except Exception:
    print(0)
EOF
      )
    fi

    [[ "$status" == "200" ]] && return 0

    sleep "$interval"
    (( elapsed += interval ))
  done

  return 1
}