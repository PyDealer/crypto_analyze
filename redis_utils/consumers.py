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


async def signal_consumer(indicator: str = 'volume'):
    candle_interval = 30
    period = 48
    while True:
        response = await redis.xreadgroup(
            streams={f'stream:{indicator}': '>'},
            consumername=f'{indicator}_consumer_1',
            groupname=f'{indicator}_consumers',
            count=1)
        #  response = [['stream:volume', [('1711487797469-0', {'SCRTUSDT': '2757'})]]]
        if len(response) == 0:
            print(len(response), f'{indicator}_consumer is empty')
            await asyncio.sleep(10)
        else:
            symbol = response[0][1][0][1].keys()
            symbol = list(symbol)[0]
            arg = response[0][1][0][1][symbol]
            timestamp_stream = response[0][1][0][0]

            graph = KlinesGraph([symbol], candle_interval, period)
            await graph.get_graph(
                img_id=timestamp_stream, rsi=True, volume=True)
            message = SignalMessage(
                symbol, indicator, timestamp_stream, arg=arg)
            try:
                await message.send_to_group()
            except telegram.error.TimedOut:
                await asyncio.sleep(20)
                await message.send_to_group()
