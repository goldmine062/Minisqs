#!/usr/bin/env bash
# Kills the broker with SIGKILL in the middle of a send burst, restarts it, and checks
# that every send the broker acknowledged is still there.
set -euo pipefail
cd "$(dirname "$0")"
DATA=crash-data
rm -rf "$DATA" acked.txt

java -jar minisqs.jar server --data-dir "$DATA" --port 8090 > crash-server.log 2>&1 &
SERVER=$!
sleep 2

java -jar minisqs.jar crashtest-send --url http://localhost:8090 --count 1000000 &
SENDER=$!
sleep "${1:-5}"

echo ">>> kill -9 broker (pid $SERVER)"
kill -9 "$SERVER"
wait "$SENDER" || true

echo ">>> restarting broker"
java -jar minisqs.jar server --data-dir "$DATA" --port 8090 > crash-server.log 2>&1 &
SERVER=$!
sleep 2
head -1 crash-server.log

java -jar minisqs.jar crashtest-verify --url http://localhost:8090
kill "$SERVER"
