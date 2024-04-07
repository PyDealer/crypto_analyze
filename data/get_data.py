from datetime import datetime as dt
from datetime import timedelta
from requests import ReadTimeout

import pandas as pd

from api_connect.http_connect import session
from redis_utils.proscessing import TimeSeries


class Symbol:
    datetime_now = dt.now()
    timestamp_end = int(datetime_now.timestamp() * 1000)

    def __init__(self,
                 symbols: list,
                 interval: int = 5,
                 period_in_hours: int = 24,
                 end_time: int = timestamp_end) -> None:
        period = dt.now() - timedelta(hours=period_in_hours)
        print(end_time)
        timestamp_start = int(period.timestamp() * 1000)
        self.symbols = symbols
        self.symbol = symbols[0]
        self.interval = interval
        self.start_time = timestamp_start
        self.end_time = end_time
        self.data = None
        self.df = None
        self.plots: list = []


class Klines(Symbol):
    async def get_historical_klines(self) -> dict:
        '''Запрос информации о свечах'''
        data = session.get_kline(
            category="linear",
            symbol=self.symbol,
            interval=self.interval,
            limit=1000,
            start=self.start_time,
            end=self.end_time,
        )
        result_data = data['result']['list']
        self.data = result_data
        print(data['result']['symbol'], data['retMsg'], data['retCode'])
        return result_data

    async def create_df(self):
        columns = [
            'date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        df = pd.DataFrame(self.data, columns=columns)
        df = df.sort_values(by='date').reset_index(drop=True)
        self.df = df
        return df

    async def processed_df(self):
        df = self.df
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['turnover'] = df['turnover'].astype(float)
        df['date'] = df['date'].astype('int64')
        df.set_index('date', inplace=True)
        return df

    async def convert_date_in_df(self):
        df = self.df
        df.reset_index(inplace=True)
        df['date'] = pd.to_datetime(
            df['date'], unit='ms').dt.tz_localize(
                'UTC').dt.tz_convert('Europe/Moscow')
        df.set_index('date', inplace=True)
        return df

#    async def save_df(self):
#        dfs30[self.symbol] = self.df

    async def many_data_to_redis_ts(self, indicator: str = 'volume'):
        ts = TimeSeries()
        data_for_madd = []
        for _, row in self.df.iterrows():
            data_for_madd.extend(
                [f'{self.symbol}:{indicator}',
                 int(row['date']),
                 float(row[indicator])])
        await ts.create_timeseries(self.symbol, indicator)
        await ts.madd_timeseries(data_for_madd)

    async def first_launch(self):
        error_symbols = []
        for symbol in self.symbols:
            self.symbol = symbol

            try:
                await self.get_historical_klines()
            except ReadTimeout:
                print(self.symbol, 'Error')
                error_symbols.append(self.symbol)

            await self.create_df()
            if self.interval == 5:
                await self.many_data_to_redis_ts()
                await self.many_data_to_redis_ts('close')
    
        if error_symbols:
            print('try error symbols')
            for symbol in error_symbols:
                self.symbol = symbol

                try:
                    await self.get_historical_klines()
                    await self.create_df()
                    if self.interval == 5:
                        await self.many_data_to_redis_ts()
                        await self.many_data_to_redis_ts('close')
                except ReadTimeout:
                    print(self.symbol, 'Error again')
