import pytest
from httpx import AsyncClient
import uuid
import json
import fakeredis.aioredis

pytestmark = pytest.mark.asyncio

async def test_valid_pulse_enqueued(async_client: AsyncClient, fake_redis: fakeredis.aioredis.FakeRedis):
    session_id = str(uuid.uuid4())
    payload = {
        "session_id": session_id,
        "user_id": "usr123",
        "content_id": "movie_99",
        "playback_position_sec": 120,
        "bitrate_kbps": 4000,
        "buffer_duration_sec": 0.5,
        "event_type": "heartbeat"
    }

    response = await async_client.post("/v1/telemetry/pulse", json=payload)
    
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "queued"
    assert data["session_id"] == session_id

    messages = await fake_redis.xrange("stream:telemetry:pulses")
    assert len(messages) == 1
    
    msg_id, fields = messages[0]
    queued_payload = json.loads(fields["payload"])
    assert queued_payload["session_id"] == session_id
    assert queued_payload["content_id"] == "movie_99"

async def test_malformed_pulse_rejected(async_client: AsyncClient, fake_redis: fakeredis.aioredis.FakeRedis):
    payload = {
        "session_id": "invalid-uuid",
        "user_id": "us", # too short
        "content_id": "movie_99",
        "playback_position_sec": -10, # invalid
        "bitrate_kbps": 0, # invalid
        "buffer_duration_sec": -1.0, # invalid
        "event_type": "invalid_event" # invalid
    }

    response = await async_client.post("/v1/telemetry/pulse", json=payload)
    assert response.status_code == 422
    
    messages = await fake_redis.xrange("stream:telemetry:pulses")
    assert len(messages) == 0
