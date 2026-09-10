# Stream Forge — Frontend ↔ Backend API Contract (Draft v1)

This is what the dashboard currently expects. Share this with your backend
partner so their FastAPI layer matches what the frontend already renders —
no frontend changes needed once this is implemented for real.

## Worker shape

Every worker the frontend renders needs exactly this shape:

```json
{
  "id": 1,
  "name": "Worker 1",
  "status": "Running",
  "load": 42,
  "messages": 12
}
```

- `status` is one of `"Running"` | `"Stopped"` (exact casing matters — the
  frontend does a strict string match right now).
- `load` is a 0–100 integer percentage. If the backend only has raw
  throughput/lag numbers, it needs to translate to a 0–100 scale before
  sending — the frontend does not currently do that conversion.
- `messages` is a running total of messages that worker has processed
  since it last started.

## REST endpoints needed

| Method | Path | Returns | Notes |
|---|---|---|---|
| GET | `/api/workers` | `Worker[]` | Initial state on dashboard load |
| POST | `/api/workers/{id}/toggle` | updated `Worker` | Manually stop/start a worker (this should trigger a real Faust rebalance + RocksDB changelog recovery on the backend side, per the Week 3 plan) |

## Real-time feed (WebSocket)

`WS /ws/stream`

The backend pushes one JSON message per event, either:

```json
{ "type": "message_processed", "workerId": 1, "loadDelta": 4, "timestamp": 1735689600 }
```
```json
{ "type": "worker_status", "workerId": 3, "status": "Stopped", "timestamp": 1735689600 }
```

- `message_processed` → frontend increments that worker's message count
  and load by `loadDelta`.
- `worker_status` → frontend flips that worker's status, which triggers
  the rebalance-timeline segment, the reroute animation on the graph,
  and the activity log entry — all already built and don't need backend
  changes to work, as long as this event shape is what gets sent.

## Open questions for backend

- Does Faust naturally expose a "worker went down / rebalanced" event we
  can forward over the WebSocket, or do we need a small wrapper that
  watches partition assignment changes and emits it?
- Is `load` going to be CPU%, consumer lag, or something else? Whatever
  it is, it needs to be normalized to 0–100 before it reaches the
  frontend, or the load bars/colors and alert thresholds (60%/85%) won't
  mean anything real.
- Total `messages` per worker — does RocksDB state give us that for free,
  or does the backend need to track it separately?