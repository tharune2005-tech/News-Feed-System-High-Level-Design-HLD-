#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="."
cmd="${1:-serve}"
if [[ "$cmd" == "test" ]]; then
  python3 tests/test_newsfeed.py
else
  python3 -m newsfeed.main "${1:-8080}"
fi
