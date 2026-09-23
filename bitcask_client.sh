#!/usr/bin/env bash
# Thin wrapper so the client can be run as ./bitcask_client.sh ...
# Delegates to the Python client sitting next to this script.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/bitcask_client.py" "$@"