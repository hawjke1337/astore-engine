#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/astore-engine/app"
SERVICE_NAME="astore-engine"

cd "$APP_DIR"
git pull origin main

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager
