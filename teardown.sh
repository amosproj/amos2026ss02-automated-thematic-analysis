#!/usr/bin/env bash
# Convenience wrapper — equivalent to: ./setup.sh --down
# Pass -y / --yes to also skip the confirmation prompt.
# Pass --down-volumes to remove data volumes as well.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/setup.sh" --down "$@"