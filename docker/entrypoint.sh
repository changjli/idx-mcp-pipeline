#!/bin/sh
set -e
# Redis: in-memory only (no AOF/RDB). State is transient by design —
# pending tasks lost on restart, acceptable for v1 (idempotent upserts +
# scheduler re-fires next tick + manual backfill via local stack).
redis-server --daemonize yes --save "" --appendonly no \
  --dir /tmp --logfile /dev/null --bind 127.0.0.1 --port 6379

exec ./mcp-server