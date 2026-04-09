#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="$(cd "$(dirname "$0")" && pwd)"
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

