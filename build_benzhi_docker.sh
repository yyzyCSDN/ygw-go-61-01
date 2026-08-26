#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

docker build -f benzhi.Dockerfile -t sessionstore:local .
echo "built sessionstore:local"
