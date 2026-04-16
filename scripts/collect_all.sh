#!/usr/bin/env bash
set -euo pipefail

if [ -d "/app" ]; then
    cd /app
else
    cd "$(dirname "$0")/.."
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting collection cycle..."
python3 scripts/collect_funding.py
python3 scripts/collect_orderbook.py
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Collection complete."
