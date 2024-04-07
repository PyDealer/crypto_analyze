import asyncio
import pandas_ta as ta
import pandas as pd

from redis_utils.producers import volume_producer
from redis_utils.proscessing import Hash, Stream, TimeSeries, String
from .get_data import Symbol, Klines


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


#class RSI(Klines):
#    async def add_rsi(self, symbol: str):
#        if not dfs30[symbol].empty:
#            dfs30[symbol]['rsi'] = ta.rsi(dfs30[symbol]['close'], length=14)
#        #print(dfs30[symbol].tail(1))
#            print(dfs30[symbol]['rsi'].iloc[-1])
#        else:
#            print(symbol)
#
#    async def drop_last_row_rsi(self, symbol: str):
#        if not dfs30[symbol].empty:
#            dfs30[symbol].drop(dfs30[symbol].index[0], inplace=True)
#
#    async def pipeline_rsi(self, symbol: str):
#        await self.drop_last_row_rsi(symbol)
#        await self.add_rsi(symbol)
#
#
#class Indicators(RSI):
#    async def update_rsi(self):
#        for symbol in self.symbols:
#            self.symbol = symbol
#            asyncio.create_task(self.pipeline_rsi(symbol))
#
#
#class Signal(Indicators):
#    async def check_rsi(self):
#        for symbol in self.symbols:
#            if not dfs30[symbol].empty:
#                rsi = dfs30[symbol]['rsi'].iloc[-1]
#                if rsi < 30 or rsi > 70:
#                    print(symbol, rsi)
#            else:
#                print(symbol)
