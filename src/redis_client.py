from redis import Redis
from scrapy.settings import BaseSettings


def get_redis(settings: BaseSettings) -> Redis:
    return Redis(
        host=settings.get("REDIS_HOST"),
        port=settings.getint("REDIS_PORT"),
        db=settings.getint("REDIS_DB"),
        password=settings.get("REDIS_PASSWORD"),
        decode_responses=True,
    )