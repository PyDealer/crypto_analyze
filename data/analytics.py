import pandas_ta as ta
import pandas as pd

from redis_utils.producers import volume_producer, rsi_producer
from redis_utils.proscessing import RedisMethods


class DataFrame:
    def __init__(self) -> None:
        self.df = None

    async def create_df(self, data: list[list], columns: list[str]):
        df = pd.DataFrame(data, columns=columns)
        return df


class RSI:
    def __init__(self) -> None:
        pass

    async def add_rsi(self, df):
        df['close'] = df['close'].astype(float)
        df['rsi'] = ta.rsi(df['close'], length=14)


class Indicators(DataFrame, RSI):
    def __init__(self) -> None:
        pass


class Analytics(RedisMethods):
    def __init__(self) -> None:
        pass

    async def check_anomaly_volume(self, symbol: str):
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

    async def check_rsi(self, symbol: str):
        timestamp_close = await self.range_aggregate(symbol, 'close', 'last', 30*60*1000)
        #print(symbol, timestamp_close)
        if timestamp_close:
            indicator = Indicators()
            df = await indicator.create_df(timestamp_close, ['date', 'close'])
            await indicator.add_rsi(df)

            rsi = df['rsi'].iloc[-1]
            if rsi is not None and (rsi < 30 or rsi > 70):
            #if rsi is not None and rsi < 30:
                #await self.set_expire_signal(symbol, 'rsi', 60*15)
                #await rsi_producer(symbol, rsi)
                print(symbol, rsi)
