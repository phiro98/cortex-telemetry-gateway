import json
from app.core.redis import get_redis_client
from app.core.config import settings
from app.schemas.telemetry import PlaybackPulseEvent

async def publish_pulse_event(event: PlaybackPulseEvent) -> str:
    redis_client = get_redis_client()
    event_dict = event.model_dump(mode='json')
    # Use JSON string serialization for the event data inside the Redis stream
    message_id = await redis_client.xadd(
        name=settings.STREAM_NAME,
        fields={"payload": json.dumps(event_dict)}
    )
    return message_id
