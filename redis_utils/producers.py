from app.config import redis


async def volume_producer(symbol: str, value: int, indicator: str = 'volume'):
    res = await redis.xadd(
        f'stream:{indicator}',
        {symbol: value},
        maxlen=400,
        approximate=True)
