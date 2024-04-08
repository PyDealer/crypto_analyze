import asyncio

from redis_utils.proscessing import TimeSeries, Hash
from data.analytics import Analytics

SLEEP_COUNT_UPDATE_VOLUME_SCHEDULER = 60*10
SLEEP_COUNT_CHECK_ANOMALY_VOLUME_SCHEDULER = 10
SLEEP_COUNT_CHECK_RSI_SCHEDULER = 60*3

AVG_PERIOD = 60*60*1*24*1000


async def update_volume_scheduler(sympols: list):
    await asyncio.sleep(10)
    ts = TimeSeries()
    volume_hash = Hash()
    while True:
        print('update_volume_scheduler awake')
        for symbol in sympols:
            avg_value = await ts.range_aggregate(
                symbol, 'volume', 'avg', AVG_PERIOD)
            if avg_value:
                await volume_hash.update_volume(symbol, int(float(avg_value[-1][1])))
        await asyncio.sleep(SLEEP_COUNT_UPDATE_VOLUME_SCHEDULER)


async def check_anomaly_volume_scheduler(sympols: list):
    await asyncio.sleep(15)
    analytics = Analytics()
    while True:
        print('check_anomaly_volume_scheduler awake')
        for symbol in sympols:
            #print(symbol)
            await analytics.check_anomaly_volume(symbol)
        await asyncio.sleep(SLEEP_COUNT_CHECK_ANOMALY_VOLUME_SCHEDULER)


async def check_rsi_scheduler(sympols: list):
    await asyncio.sleep(15)
    analytics = Analytics()
    while True:
        print('check_rsi_scheduler awake')
        for symbol in sympols:
            #print(symbol)
            await analytics.check_rsi(symbol)
        await asyncio.sleep(SLEEP_COUNT_CHECK_RSI_SCHEDULER)
