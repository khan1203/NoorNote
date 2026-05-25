import os
import json
from redis import asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
CACHE_TTL = int(os.getenv("CACHE_TTL", 3600))

redis_client: aioredis.Redis = None


async def connect_to_redis():
    global redis_client
    try:
        redis_client = await aioredis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True
        )
        await redis_client.ping()
        print("Connected to Redis")
    except  Exception as e:
        print(f"Redis connection FAILED: {e}")  # check this in logs


async def close_redis_connection():
    global redis_client
    if redis_client:
        await redis_client.aclose()
        print("Closed Redis connection")


def get_redis():
    return redis_client


async def cache_get(key: str):
    redis = get_redis()
    cached = await redis.get(key)
    if cached:
        return json.loads(cached)
    return None


async def cache_set(key: str, value: dict, ttl: int = CACHE_TTL):
    redis = get_redis()
    await redis.set(key, json.dumps(value), ex=ttl)


async def cache_delete(key: str):
    redis = get_redis()
    await redis.delete(key)


async def cache_delete_pattern(pattern: str):
    redis = get_redis()
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys)
