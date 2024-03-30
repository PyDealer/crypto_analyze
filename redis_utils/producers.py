from app.config import redis


async def volume_producer(symbol: str, value: int, indicator: str = 'volume'):
    #await redis.xgroup_create(f'stream:{indicator}', f'{indicator}_consumers', '$',  mkstream=True)
    res = await redis.xadd(
        f'stream:{indicator}',
        {symbol: value},
        maxlen=400,
        approximate=True)
