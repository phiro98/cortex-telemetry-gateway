import json
import asyncio
from typing import List, Tuple
from collections import defaultdict

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.core.config import settings
from app.core.redis import get_redis_client
from app.models.metrics import ContentHourlyMetric
from app.schemas.telemetry import PlaybackPulseEvent

async def init_consumer_group(redis_client: redis.Redis):
    try:
        await redis_client.xgroup_create(
            name=settings.STREAM_NAME,
            groupname=settings.CONSUMER_GROUP,
            id="$",
            mkstream=True
        )
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise e

async def process_batch(db_session: AsyncSession, messages: List[Tuple[str, dict]]):
    if not messages:
        return []

    aggregates = defaultdict(lambda: {"total_play_time_sec": 0, "total_buffer_duration_sec": 0.0, "pulse_count": 0})
    message_ids = []

    for message_id, fields in messages:
        message_ids.append(message_id)
        if "payload" not in fields:
            continue
            
        try:
            payload = json.loads(fields["payload"])
            event = PlaybackPulseEvent(**payload)
            
            # Truncate to hour
            hour_bucket = event.timestamp.replace(minute=0, second=0, microsecond=0)
            
            key = (event.content_id, hour_bucket)
            aggregates[key]["total_play_time_sec"] += event.playback_position_sec
            aggregates[key]["total_buffer_duration_sec"] += event.buffer_duration_sec
            aggregates[key]["pulse_count"] += 1
            
        except Exception as e:
            # In a real system, log the error and move to a dead letter queue
            print(f"Error parsing message {message_id}: {e}")
            continue

    if not aggregates:
        return message_ids

    # Prepare data for upsert
    insert_values = []
    for (content_id, hour_bucket), metrics in aggregates.items():
        insert_values.append({
            "content_id": content_id,
            "hour_bucket": hour_bucket,
            "total_play_time_sec": metrics["total_play_time_sec"],
            "total_buffer_duration_sec": metrics["total_buffer_duration_sec"],
            "pulse_count": metrics["pulse_count"]
        })
        
    if "postgres" in settings.DATABASE_URL:
        stmt = pg_insert(ContentHourlyMetric).values(insert_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['content_id', 'hour_bucket'],
            set_={
                'total_play_time_sec': ContentHourlyMetric.total_play_time_sec + stmt.excluded.total_play_time_sec,
                'total_buffer_duration_sec': ContentHourlyMetric.total_buffer_duration_sec + stmt.excluded.total_buffer_duration_sec,
                'pulse_count': ContentHourlyMetric.pulse_count + stmt.excluded.pulse_count
            }
        )
    elif "sqlite" in settings.DATABASE_URL:
        stmt = sqlite_insert(ContentHourlyMetric).values(insert_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['content_id', 'hour_bucket'],
            set_={
                'total_play_time_sec': ContentHourlyMetric.total_play_time_sec + stmt.excluded.total_play_time_sec,
                'total_buffer_duration_sec': ContentHourlyMetric.total_buffer_duration_sec + stmt.excluded.total_buffer_duration_sec,
                'pulse_count': ContentHourlyMetric.pulse_count + stmt.excluded.pulse_count
            }
        )
    else:
        raise ValueError(f"Unsupported database dialect for UPSERT: {settings.DATABASE_URL}")

    await db_session.execute(stmt)
    await db_session.commit()

    return message_ids


async def run_worker(db_session_maker, batch_size=100, block_ms=2000):
    redis_client = get_redis_client()
    await init_consumer_group(redis_client)
    
    print(f"Starting consumer worker: {settings.CONSUMER_NAME}")
    
    while True:
        try:
            # Read from stream
            streams = {settings.STREAM_NAME: ">"}
            response = await redis_client.xreadgroup(
                groupname=settings.CONSUMER_GROUP,
                consumername=settings.CONSUMER_NAME,
                streams=streams,
                count=batch_size,
                block=block_ms
            )
            
            if not response:
                continue
                
            stream_name, messages = response[0]
            if not messages:
                continue
                
            # Process batch
            async with db_session_maker() as db_session:
                processed_ids = await process_batch(db_session, messages)
                
            # Acknowledge messages
            if processed_ids:
                await redis_client.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, *processed_ids)
                
        except asyncio.CancelledError:
            print("Worker cancelled.")
            break
        except Exception as e:
            print(f"Worker error: {e}")
            await asyncio.sleep(1) # Prevent tight loop on continuous errors
