import aioredis

from app.config import redis
from .producers import volume_producer


class TimeSeries:
    def __init__(self):
        pass

    async def create_timeseries(self, key, indicator):
        try:
            await redis.execute_command(
                'TS.CREATE', f'{key}:{indicator}',
                'DUPLICATE_POLICY', 'last',
                'RETENTION', 60*60*24*3*1000
            )
        except aioredis.exceptions.ResponseError:
            pass

    async def add_timeseries(self, key, indicator, ts, value):
        try:
            await redis.execute_command(
                'TS.ADD', f'{key}:{indicator}',
                ts,
                value,
                'RETENTION', 60*60*24*3*1000
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

    async def get_avg(self, symbol: str, indicator: str = 'volume', period: int = 60*60*24*1*1000):
        try:
            value = await redis.execute_command(
                'TS.RANGE', f'{symbol}:{indicator}',
                '-', '+',
                'AGGREGATION', 'AVG',
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


class VolumeAnalytics(Hash, Stream, TimeSeries, String):
    def __init__(self) -> None:
        super().__init__()

    async def check_anomaly(self, symbol: str):
        avg_volume = await self.get_avg_volume(symbol)
        last_volume = await self.get_last(symbol, 'volume')
        if last_volume:
            last_volume = int(float(last_volume[1]))
        if avg_volume:
            avg_volume = int(float(avg_volume))
        if ((avg_volume and last_volume
             ) and ((avg_volume*3) < last_volume)):
            signal_status = await self.get_expire_signal(symbol, 'volume')
            if not signal_status:
                print(f'Signal! diff: {round(last_volume/avg_volume, 1)} {symbol}: {last_volume} avg:{avg_volume}')
                await self.set_expire_signal(symbol, 'volume', 60*15)
                await volume_producer(symbol, last_volume) #  +++ diff ++ comments
