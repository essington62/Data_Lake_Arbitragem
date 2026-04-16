#!/usr/bin/env bash
set -euo pipefail

if [ -d "/app" ]; then
    cd /app
else
    cd "$(dirname "$0")/.."
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting funding rate collection..."
python scripts/collect_funding.py
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Collection complete."
