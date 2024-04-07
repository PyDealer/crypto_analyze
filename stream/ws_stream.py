import asyncio
from typing import Callable

from api_connect.ws_kline import WSKline
from redis_utils.proscessing import TimeSeries


class KlineStream:
    def __init__(self,
                 symbol: list,
                 type_of_stream: str = 'to_redis_5',
                 interval: int = 5) -> None:
        self.symbol: list = symbol
        self.ws_kline = WSKline(self.symbol)
        self.redis_ts = TimeSeries()
        self.type_of_stream = type_of_stream
        self.interval = interval

    async def stream(self):
        '''Запуск стрима'''
        await self.ws_kline.stream(self.interval)
        #await self.ws_kline.check_condition()

    async def cycle(self, func: Callable):
        '''Шедулер стрима всех данных'''
        while True:
            data = await self.ws_kline.get_data()
            if data:
                # 'data' = ('kline.5.10000LADYSUSDT', [{'start': 1711759800000, 'end': 1711760099999, 'interval': '5', 'open': '0.0025689', 'close': '0.0025733', 'high': '0.0025767', 'low': '0.0025676', 'volume': '3577200', 'turnover': '9203.29365', 'confirm': False, 'timestamp': 1711759969798}])
                asyncio.create_task(func(data))
            await asyncio.sleep(0.1)

    async def extract_topic(self, topic):
        '''Получает чистое название монеты'''
        if topic:
            coin = topic.split('.')[-1]
            return coin

    async def update_ts(self, data):
        coin = await self.extract_topic(data[0])
        timestamp_end = data[1][0]['end']
        volume = data[1][0]['volume']
        close = data[1][0]['close']
#        await self.redis_ts.create_timeseries(coin, 'volume')
        await self.redis_ts.add_timeseries(
            coin, 'volume', timestamp_end, volume)
        await self.redis_ts.add_timeseries(
            coin, 'close', timestamp_end, close)

    async def starter(self):
        '''Старт стрима в рамках объекта'''
        await self.stream()
        if self.type_of_stream == 'to_redis_5':
            await self.cycle(self.update_ts)


async def launch_stream(symbol_list):
    volume_stream = KlineStream(symbol_list, 'to_redis_5', 5)
    asyncio.create_task(volume_stream.starter())
