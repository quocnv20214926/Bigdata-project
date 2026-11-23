#!/usr/bin/env sh
# Whole system with one command (week 8): base + batch + stream compose files.
#   scripts/stack.sh up -d --build      scripts/stack.sh ps      scripts/stack.sh down
# PowerShell: . .\scripts\load-env.ps1, then  dcall up -d --build
cd "$(dirname "$0")/.." || exit 1
[ -f .env ] || cp .env.example .env
exec docker compose --env-file .env -f infra/compose.base.yml -f infra/compose.batch.yml -f infra/compose.stream.yml "$@"
