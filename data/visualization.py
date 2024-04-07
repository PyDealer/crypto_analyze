import pandas_ta as ta
import mplfinance as mpf
import matplotlib as mpl

from .get_data import Symbol, Klines
mpl.use('Agg')


class Graph(Symbol):
    async def rsi(self):
        self.df['rsi'] = ta.rsi(self.df['close'], length=14)
        rsi_plot = mpf.make_addplot(
            self.df['rsi'],
            panel=2,
            ylabel='RSI',
            color='#7EB4BB',
            ylim=(10, 110),
            )
        self.plots.append(rsi_plot)

    async def volume(self):
        volume_plot = mpf.make_addplot(
            self.df['volume'],
            panel=1,
            ylabel='Volume',
            type='bar',
            color='#7EB4BB',
            secondary_y='auto',
            )
        self.plots.append(volume_plot)

    async def create_kline_graphic(self, img_id):
        mpf.plot(
            self.df, type='candle',
            addplot=self.plots,
            style='charles',
            figsize=(12, 8),
            scale_padding=dict(left=0.1, bottom=0.3, top=0.3),
            datetime_format='%d.%m %H:%M', xrotation=0,
            title=f'{self.symbol} {self.interval}. @PyDealer',
            savefig=f'media/signals/{self.symbol}_{img_id}.png')


class KlinesGraph(Klines, Graph):
    async def get_graph(self, img_id, rsi=False, volume=False):
        await self.get_historical_klines()
        await self.create_df()
        await self.processed_df()
        await self.convert_date_in_df()
        await self.rsi() if rsi else None
        await self.volume() if volume else None
        await self.create_kline_graphic(img_id)
