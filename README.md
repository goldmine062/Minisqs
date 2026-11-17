# MiniSQS

A durable message queue modelled on Amazon SQS, written in plain Java 21 with no external libraries.

It supports at-least-once delivery, visibility timeouts, dead-letter queues, delayed messages, deduplication, long polling and batching. State is kept in memory and made durable with a write-ahead log that survives crashes, repairs torn writes and is compacted in the background. It comes with a live dashboard, Prometheus metrics, a test suite, a load tester and a crash tester.

## Requirements

JDK 21 or newer (check with `javac -version`). Nothing else: no Maven, no Docker, no database.

## Build

```bash
./build.sh          # macOS / Linux / Git Bash
.\build.ps1         # Windows PowerShell
```

This produces `minisqs.jar`. You can also open the folder in IntelliJ (it picks up `pom.xml`) and run `com.minisqs.Cli`, passing the command (for example `server`) as the program argument.

## See it in action

Open four terminals in the project folder.

**1. Start the broker**
```bash
java -jar minisqs.jar server
```
Then open http://localhost:8080 for the dashboard.

**2. Start a worker that crashes after 2 messages**
```bash
java -jar minisqs.jar worker --queue orders --name w1 --crash-after 2
```

**3. Start a worker that fails 30% of the time**
```bash
java -jar minisqs.jar worker --queue orders --name w2 --fail-rate 0.3
```

**4. Send 20 orders**
```bash
java -jar minisqs.jar produce --queue orders --count 20
```
The producer creates `orders` (10 s visibility timeout, max 3 receives) and its dead-letter queue `orders-dlq`. Start it before the workers if they complain that the queue doesn't exist; they retry every 2 seconds anyway.

What to watch:

- w1 takes a message and dies without deleting it. On the dashboard that message sits in **in flight** for 10 seconds, then becomes **visible** again and w2 picks it up with `attempt 2`. Nothing was lost.
- w2's failed messages come back the same way. A message that fails 3 times is moved to `orders-dlq` instead of blocking the queue forever.
- Stop the broker (Ctrl+C) in the middle and restart it. It prints how many messages it recovered from the log, and the workers carry on.

You can also send and receive by hand from the dashboard, or with curl (on Windows PowerShell type `curl.exe`, not `curl`):

```bash
curl -X POST "localhost:8080/api/queues?name=emails&visibilityTimeout=30"
curl -X POST "localhost:8080/api/queues/emails/messages" -d "welcome mail for user 7"
curl -X POST "localhost:8080/api/queues/emails/receive?max=1&wait=5"
curl -X DELETE "localhost:8080/api/queues/emails/messages?receiptHandle=PASTE_HANDLE_HERE"
curl localhost:8080/metrics
```

## Tests, load test, crash test

```bash
java -jar minisqs.jar selftest
```
Twelve tests: basic flow, visibility timeout, stale receipt handles, dead-letter queue, delays, deduplication, releasing a message, restart recovery, torn-write repair, compaction, long polling and a concurrent producer/consumer run. A fake clock makes the timeout tests instant.

```bash
java -jar minisqs.jar loadtest --producers 8 --consumers 8 --seconds 20 --batch 10
```
Reports send throughput, consume throughput, p50/p99 latencies, duplicates and lost messages. Try `--batch 1` versus `--batch 10`, and starting the server with `--fsync false`, to see what batching and fsync cost. Run it on your own machine and record your numbers.

```bash
./crash-test.sh          # macOS / Linux / Git Bash
```
Starts a broker, floods it with sends, kills it with `kill -9`, restarts it and checks that every acknowledged send is still there. On Windows do it by hand: run `server`, run `crashtest-send` in a second terminal, kill the server's `java.exe` in Task Manager, restart the server, then run `crashtest-verify`.

## API

Times are in seconds, like SQS.

| Method and path | What it does |
|---|---|
| `POST /api/queues?name=&visibilityTimeout=&maxReceiveCount=&dlq=&delay=` | Create a queue (idempotent) |
| `GET /api/queues`, `GET /api/queues/{q}` | Stats |
| `DELETE /api/queues/{q}` | Delete a queue |
| `POST /api/queues/{q}/messages?delay=&dedupId=` | Send; the body is the message |
| `POST /api/queues/{q}/messages/batch` | Send up to 10; body is a JSON array of strings |
| `POST /api/queues/{q}/receive?max=&wait=&visibilityTimeout=` | Receive up to 10; `wait` up to 20 enables long polling |
| `DELETE /api/queues/{q}/messages?receiptHandle=` | Delete (acknowledge) a message |
| `POST /api/queues/{q}/delete-batch` | Delete up to 10; body is a JSON array of handles |
| `POST /api/queues/{q}/visibility?receiptHandle=&timeout=` | Extend a lease, or `timeout=0` to give the message back |
| `POST /api/admin/compact` | Compact the log now |
| `GET /api/info`, `GET /metrics` | Log and recovery info, Prometheus metrics |

## How it works

```
 producers ──HTTP──▶ ┌────────────── Broker ──────────────┐
                     │  queue "orders"    queue "orders-dlq"│
 consumers ◀─HTTP──▶ │  ┌──────────────┐  ┌──────────────┐ │
  (long poll)        │  │ schedule     │  │ schedule     │ │
                     │  │ (sorted by   │  │              │ │
                     │  │  visibleAt)  │  │              │ │
                     │  │ byId, byRcpt │  │              │ │
                     │  └──────┬───────┘  └──────┬───────┘ │
                     │         └──── append ─────┘         │
                     │          write-ahead log ──fsync──▶ disk
                     └─────────────────────────────────────┘
```

**One sorted set per queue.** Every message is in a `TreeSet` ordered by `visibleAt`. A new message has `visibleAt = now` (or `now + delay`). Receiving it sets `visibleAt = now + visibilityTimeout`. So ready, delayed and in-flight messages are not separate structures, just different `visibleAt` values. Receive takes from the front while `visibleAt <= now`. An expired visibility timeout needs no background scanner: the message simply sorts back into the "due" part of the set once its time has passed. Receive is O(log n) per message.

**Receipt handles.** Each receive generates a fresh random handle and invalidates the previous one. If a slow worker's timeout expires and another worker receives the message, the slow worker's delete is rejected (HTTP 410). This is why consumers must be idempotent: the slow worker may have finished the work anyway.

**Write-ahead log.** Every change is appended to `data/queue.wal` as `[length][crc32][type][payload]`, while holding the queue's lock, so the log order matches the order in which changes happened in memory. On startup the log is replayed to rebuild every queue. If the last record is incomplete or its checksum is wrong (the process died mid-write), replay stops there and truncates the file.

**What gets fsynced.** Create-queue and send are fsynced before the client gets a response, so an acknowledged send is never lost. Receive, delete and visibility changes are written but not individually fsynced. If they are lost in a power cut, the effect is a message being delivered again, which at-least-once delivery already allows. This avoids an fsync on the busiest calls.

**Group commit.** Many threads append, then call `sync`. One thread performs the fsync; others whose records were already written before that fsync started return without doing their own. Under load, one fsync covers many sends. The `walFsyncs` and `walAppends` numbers on the dashboard show the ratio.

**Compaction.** The log keeps growing because it records history. When it passes a size threshold and has doubled since the last compaction, the broker writes a new log containing only the current state (one record per live message), fsyncs it, and atomically renames it over the old one. A crash at any point leaves either the old or the new file intact. This is the same idea as compaction in an LSM tree.

**Concurrency.** Each queue has its own lock, so different queues never block each other. A broker-wide read-write lock lets normal operations run in parallel while queue creation, deletion and compaction get exclusive access. Long-polling receivers wait on a per-queue condition variable outside the broker lock, and each HTTP request runs on a Java 21 virtual thread, so thousands of waiting receivers cost very little.

**Dead-letter queues.** When a visible message has already been received `maxReceiveCount` times, the next receive attempt moves it to the dead-letter queue instead of delivering it, as SQS does.

## Trade-offs and limitations

- **Single node.** One process holds all state; if the machine is lost, so is the data. See scaling below.
- **Memory bound.** All messages are in memory; the log is for durability, not capacity.
- **Ordering is best effort.** Messages are roughly FIFO, but redeliveries and multiple consumers break strict order (same as SQS standard queues).
- **`kill -9` does not test fsync.** When a process is killed, the operating system still writes its buffered data to disk, so the crash test passes even with `--fsync false`. Only a power loss or kernel crash shows the difference. fsync is what protects against those.
- **Dedup window across compaction** is preserved, but counters (sent, received...) reset on restart.

## How it would scale

1. **Partition queues.** Split a busy queue into partitions on different nodes. A front-end router sends each request to a partition (round-robin for sends, any non-empty partition for receives). Throughput grows with the number of partitions. This is roughly why SQS standard queues have nearly unlimited throughput but no strict ordering.
2. **Ordering where needed.** FIFO queues with a message group id: all messages of one group go to the same partition and are delivered one at a time; different groups proceed in parallel.
3. **Replication.** Each partition has a leader and replicas. A send is acknowledged after a majority has it in their log (Raft or primary-backup). If the leader fails, a replica takes over with the same log.
4. **Separate compute from storage.** Stateless API nodes in front of a replicated storage layer, scaled independently.
5. **Efficiency.** Batch APIs, long polling, and group commit (already here) reduce calls and fsyncs.
6. **Consumers.** Autoscale workers on queue depth and age of the oldest message (both exported as metrics).
7. **Protect the cluster.** Per-queue rate limits and quotas so one tenant can't overload a node; reject with backpressure instead of falling over.

## Project layout

```
src/main/java/com/minisqs/
  core/      Broker, MessageQueue, WriteAheadLog, Message, QueueConfig, ...
  server/    HTTP API, dashboard, metrics, server entry point
  json/      tiny JSON parser and writer
  tools/     producer, worker, load test, crash test, self test, HTTP client
  Cli.java   command-line entry point
```
