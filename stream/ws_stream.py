import asyncio

from api_connect.ws_kline import WSKline
from redis_utils.proscessing import TimeSeries


class KlineStream:
    def __init__(self,
                 symbol: list,
                 type_of_stream: str = 'volume',
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

    async def cycle_klines_data(self):
        '''Шедулер стрима всех данных'''
        while True:
            data = await self.ws_kline.get_data()
            if data:
                # 'data' = ('kline.5.10000LADYSUSDT', [{'start': 1711759800000, 'end': 1711760099999, 'interval': '5', 'open': '0.0025689', 'close': '0.0025733', 'high': '0.0025767', 'low': '0.0025676', 'volume': '3577200', 'turnover': '9203.29365', 'confirm': False, 'timestamp': 1711759969798}])
                asyncio.create_task(self.add_data(data))
            await asyncio.sleep(0.1)

    async def cycle_volume(self):
        '''Шедулер стрима объемов'''
        while True:
            data = await self.ws_kline.get_data()
            if data:
                asyncio.create_task(self.add_volume(data))
            await asyncio.sleep(0.1)

    async def extract_topic(self, topic):
        '''Получает чистое название монеты'''
        if topic:
            coin = topic.split('.')[-1]
            return coin

    async def add_data(self, data):
        coin = await self.extract_topic(data[0])

    async def add_volume(self, data):
        coin = await self.extract_topic(data[0])
        timestamp_end = data[1][0]['end']
        volume = data[1][0]['volume']
        await self.redis_ts.create_timeseries(coin, 'volume')
        await self.redis_ts.add_timeseries(
            coin, 'volume', timestamp_end, volume)

    async def starter(self):
        '''Старт стрима в рамках объекта'''
        stream_starter = await self.stream()
        if self.type_of_stream == 'volume':
            await self.cycle_volume()
        elif self.type_of_stream == 'klines_data':
            await self.cycle_klines_data()


async def launch_stream(symbol_list):
    volume_stream = KlineStream(symbol_list, 'volume', 5)
    data_stream = KlineStream(symbol_list, 'klines_data', 30)
    asyncio.create_task(volume_stream.starter())
    asyncio.create_task(data_stream.starter())
