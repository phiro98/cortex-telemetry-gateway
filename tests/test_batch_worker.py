import pytest
import uuid
import json
from datetime import datetime, timezone
from sqlalchemy import select
import fakeredis.aioredis

from app.models.metrics import ContentHourlyMetric
from app.services.consumer import init_consumer_group, process_batch
from app.schemas.telemetry import PlaybackPulseEvent

pytestmark = pytest.mark.asyncio

async def test_batch_worker_aggregation(fake_redis: fakeredis.aioredis.FakeRedis, db_session_maker):
    content_id = "test_movie_1"
    now = datetime.now(timezone.utc)
    
    await init_consumer_group(fake_redis)

    for i in range(10):
        event = PlaybackPulseEvent(
            session_id=uuid.uuid4(),
            user_id="user123",
            content_id=content_id,
            playback_position_sec=10,
            bitrate_kbps=4000,
            buffer_duration_sec=0.1,
            event_type="heartbeat",
            timestamp=now
        )
        await fake_redis.xadd(
            name="stream:telemetry:pulses",
            fields={"payload": json.dumps(event.model_dump(mode='json'))}
        )

    response = await fake_redis.xreadgroup(
        groupname="cortex_telemetry_group",
        consumername="cortex_worker_1",
        streams={"stream:telemetry:pulses": ">"},
        count=100
    )
    
    assert response
    stream_name, messages = response[0]
    assert len(messages) == 10

    async with db_session_maker() as db_session:
        await process_batch(db_session, messages)

    async with db_session_maker() as db_session:
        result = await db_session.execute(select(ContentHourlyMetric).where(ContentHourlyMetric.content_id == content_id))
        metric = result.scalars().first()
        
        assert metric is not None
        assert metric.content_id == content_id
        assert metric.total_play_time_sec == 100
        assert round(metric.total_buffer_duration_sec, 2) == 1.0
        assert metric.pulse_count == 10

    for i in range(5):
        event = PlaybackPulseEvent(
            session_id=uuid.uuid4(),
            user_id="user456",
            content_id=content_id,
            playback_position_sec=20,
            bitrate_kbps=5000,
            buffer_duration_sec=0.2,
            event_type="heartbeat",
            timestamp=now
        )
        await fake_redis.xadd(
            name="stream:telemetry:pulses",
            fields={"payload": json.dumps(event.model_dump(mode='json'))}
        )

    response = await fake_redis.xreadgroup(
        groupname="cortex_telemetry_group",
        consumername="cortex_worker_1",
        streams={"stream:telemetry:pulses": ">"},
        count=100
    )
    stream_name, messages = response[0]
    
    async with db_session_maker() as db_session:
        await process_batch(db_session, messages)
        
    async with db_session_maker() as db_session:
        result = await db_session.execute(select(ContentHourlyMetric).where(ContentHourlyMetric.content_id == content_id))
        metric = result.scalars().first()
        
        assert metric.total_play_time_sec == 200
        assert round(metric.total_buffer_duration_sec, 2) == 2.0
        assert metric.pulse_count == 15
