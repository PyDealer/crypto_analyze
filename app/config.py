from pydantic_settings import BaseSettings
import aioredis


class Config(BaseSettings):
    redis_url: str = 'redis://127.0.0.1:6379'


config = Config()
redis = aioredis.from_url(config.redis_url, decode_responses=True)
