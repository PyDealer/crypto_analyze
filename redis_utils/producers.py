from app.config import redis


async def volume_producer(symbol: str, value: int, indicator: str = 'volume'):
    await redis.xadd(
        f'stream:{indicator}',
        {symbol: value},
        maxlen=400,
        approximate=True)


async def rsi_producer(symbol: str, value: float, indicator: str = 'rsi'):
    await redis.xadd(
        f'stream:{indicator}',
        {symbol: value},
        maxlen=400,
        approximate=True)
