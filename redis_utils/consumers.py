import asyncio
from aioredis.exceptions import ResponseError

from app.config import redis
from data.visualization import KlinesGraph
from telegram_bot.chat import SignalMessage
import telegram


async def create_xgroup(indicator: str = 'volume'):
    try:
        await redis.xgroup_create(
            f'stream:{indicator}', f'{indicator}_consumers', '$',
            mkstream=True)
    except ResponseError:
        pass


async def volume_consumer():
    candle_interval = 30
    period = 48
    while True:
        response = await redis.xreadgroup(
            streams={'stream:volume': '>'},
            consumername='volume_consumer_1',
            groupname='volume_consumers',
            count=1)
        #  response = [['stream:volume', [('1711487797469-0', {'SCRTUSDT': '2757'})]]]
        if len(response) == 0:
            print(len(response), 'volume_consumer is empty')
            await asyncio.sleep(10)
        else:
            symbol = response[0][1][0][1].keys()
            symbol = list(symbol)[0]
            timestamp_stream = response[0][1][0][0]

            graph = KlinesGraph([symbol], candle_interval, period)
            await graph.get_graph(img_id=timestamp_stream, rsi=True, volume=True)
            message = SignalMessage(symbol, 'volume', timestamp_stream)
            try:
                await message.send_to_group()
            except (telegram.error.TimedOut):
                await asyncio.sleep(20)
                await message.send_to_group()


async def rsi_consumer():
    candle_interval = 30
    period = 48
    while True:
        response = await redis.xreadgroup(
            streams={'stream:volume': '>'},
            consumername='volume_consumer_1',
            groupname='volume_consumers',
            count=1)
        #  response = [['stream:volume', [('1711487797469-0', {'SCRTUSDT': '2757'})]]]
        if len(response) == 0:
            print(len(response), 'volume_consumer is empty')
            await asyncio.sleep(10)
        else:
            symbol = response[0][1][0][1].keys()
            symbol = list(symbol)[0]
            timestamp_stream = response[0][1][0][0]

            graph = KlinesGraph([symbol], candle_interval, period)
            await graph.get_graph(img_id=timestamp_stream, rsi=True, volume=True)
            message = SignalMessage(symbol, 'volume', timestamp_stream)
            try:
                await message.send_to_group()
            except (telegram.error.TimedOut):
                await asyncio.sleep(20)
                await message.send_to_group()
