import aioredis

from app.config import redis


class TimeSeries:
    def __init__(self) -> None:
        pass

    async def create_timeseries(
            self,
            key: str,
            indicator: str,
            retention: int = 60*60*24*3*1000):
        try:
            await redis.execute_command(
                'TS.CREATE', f'{key}:{indicator}',
                'DUPLICATE_POLICY', 'last',
                'RETENTION', retention
            )
        except aioredis.exceptions.ResponseError:
            pass

    async def add_timeseries(
            self,
            key: str,
            indicator: str,
            ts,
            value,
            retention: int = 60*60*24*3*1000):
        try:
            await redis.execute_command(
                'TS.ADD', f'{key}:{indicator}',
                ts,
                value,
                'RETENTION', retention
            )
        except aioredis.exceptions.ResponseError:
            pass

    async def madd_timeseries(self, data: list):
        try:
            await redis.execute_command(
                'TS.MADD',
                *data,
            )
        except aioredis.exceptions.ResponseError:
            print(data)

    #async def get_avg(
    #        self,
    #        symbol: str,
    #        indicator: str = 'volume',
    #        period: int = 60*60*24*1*1000):
    #    try:
    #        value = await redis.execute_command(
    #            'TS.RANGE', f'{symbol}:{indicator}',
    #            '-', '+',
    #            'AGGREGATION', 'AVG',
    #            period)
    #        return value
    #    except aioredis.exceptions.ResponseError:
    #        pass

    async def range_aggregate(
            self,
            symbol: str,
            indicator: str,
            aggregator: str,
            period: int = 60*60*24*1*1000):
        try:
            value = await redis.execute_command(
                'TS.RANGE', f'{symbol}:{indicator}',
                '-', '+',
                'AGGREGATION', aggregator,
                period)
            return value
        except aioredis.exceptions.ResponseError:
            pass

    async def get_last(self, symbol: str, indicator: str = 'volume'):
        try:
            value = await redis.execute_command(
                'TS.GET', f'{symbol}:{indicator}')
            return value
        except aioredis.exceptions.ResponseError:
            pass

    async def get_range(self, symbol: str, indicator: str = 'volume'):
        try:
            value = await redis.execute_command(
                'TS.RANGE', f'{symbol}:{indicator}',
                '-', '+')
            return value
        except aioredis.exceptions.ResponseError:
            pass


class Hash:
    def __init__(self) -> None:
        pass

    async def update_volume(self, symbol: str, value: int):
        await redis.hset('avg_volume', symbol, value)

    async def get_avg_volume(self, symbol: str):
        value = await redis.hget('avg_volume', symbol)
        return value


class Stream:
    def __init__(self) -> None:
        pass

    async def create_stream(self):
        pass


class String:
    def __init__(self) -> None:
        pass

    async def set_expire_signal(
            self, symbol: str, indicator: str, expire: int):
        await redis.set(f'signal_status:{indicator}:{symbol}', 'true', ex=expire)

    async def get_expire_signal(self, symbol: str, indicator: str):
        status = await redis.get(f'signal_status:{indicator}:{symbol}')
        return status


class RedisMethods(Hash, Stream, TimeSeries, String):
    pass
