import redis.asyncio as redis
from app.core.config import settings

pool = redis.ConnectionPool.from_url(settings.REDIS_URL, decode_responses=True)

def get_redis_client() -> redis.Redis:
    return redis.Redis.from_pool(pool)
