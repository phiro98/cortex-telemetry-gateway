# Architecture

## 1. Ingress Tier (FastAPI)
- Exposes `POST /v1/telemetry/pulse`.
- Validates data payload using `pydantic v2`.
- Asynchronously enqueues events directly to a Redis stream (`XADD`) for non-blocking sub-15ms processing.
- Returns HTTP 202 Accepted.

## 2. Event Streaming (Redis Streams)
- Provides high-throughput asynchronous message bus.
- Uses Consumer Groups for durable stateful processing and scaling of consumers.

## 3. Processing Tier (Batch Worker)
- Async Python process that reads streams using `XREADGROUP`.
- Batches events in-memory.
- Upserts the aggregated metrics hourly to PostgreSQL (`ON CONFLICT DO UPDATE`).

## 4. Storage (PostgreSQL)
- Retains aggregated metric state.
- Stores cumulative playback metrics partitioned by `content_id` and `hour_bucket`.
