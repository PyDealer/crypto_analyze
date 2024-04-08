import asyncio

from redis_utils.proscessing import TimeSeries, Hash
from data.analytics import Analytics

SLEEP_COUNT_UPDATE_VOLUME_SCHEDULER = 60*10
SLEEP_COUNT_CHECK_ANOMALY_VOLUME_SCHEDULER = 10
SLEEP_COUNT_CHECK_RSI_SCHEDULER = 60*3

AVG_PERIOD = 60*60*1*24*1000

MAX_CONCURRENT_TASKS = 1

semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)


async def update_volume_avg_value_in_hash(symbols: list):
    ts = TimeSeries()
    volume_hash = Hash()
    for symbol in symbols:
        avg_value = await ts.range_aggregate(
            symbol, 'volume', 'avg', AVG_PERIOD)
        if avg_value:
            await volume_hash.update_volume(
                symbol, int(float(avg_value[-1][1])))


async def update_volume_scheduler(symbols: list):
    await asyncio.sleep(10)
    while True:
        print('update_volume_scheduler awake')
        async with semaphore:
            await update_volume_avg_value_in_hash(symbols)
        await asyncio.sleep(SLEEP_COUNT_UPDATE_VOLUME_SCHEDULER)


async def check_indicator(sympols: list, func):
    for symbol in sympols:
        await func(symbol)


async def check_anomaly_volume_scheduler(symbols: list):
    await asyncio.sleep(15)
    analytics = Analytics()
    while True:
        print('check_anomaly_volume_scheduler awake')
        async with semaphore:
            await check_indicator(symbols, analytics.check_anomaly_volume)
        #len(semaphore._semaphore._waiters)
        await asyncio.sleep(SLEEP_COUNT_CHECK_ANOMALY_VOLUME_SCHEDULER)


async def check_rsi_scheduler(symbols: list):
    await asyncio.sleep(30)
    analytics = Analytics()
    while True:
        print('check_rsi_scheduler awake')
        async with semaphore:
            await check_indicator(symbols, analytics.check_rsi)
        await asyncio.sleep(SLEEP_COUNT_CHECK_RSI_SCHEDULER)
