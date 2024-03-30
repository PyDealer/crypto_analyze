import asyncio
from concurrent.futures import ThreadPoolExecutor

from .ws_connect import ws


class WSKline:
    def __init__(self, symbol: list) -> None:
        self.symbol: list = symbol
        self.data: list[dict] = []
        self.last_values: dict = {}

    def handle_message(self, message):
        # message = {'topic': 'kline.5.10000LADYSUSDT', 'data': [{'start': 1711757100000, 'end': 1711757399999, 'interval': '5', 'open': '0.0025568', 'close': '0.0025606', 'high': '0.0025606', 'low': '0.0025552', 'volume': '4671000', 'turnover': '11949.26258', 'confirm': False, 'timestamp': 1711757238798}], 'ts': 1711757238798, 'type': 'snapshot'}
        self.data.append(message)
        self.last_values[message['topic']] = message['data']

    async def stream(self, interval: int = 5):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            loop.run_in_executor(pool, lambda: ws.kline_stream(
                interval=interval,
                symbol=self.symbol,
                callback=self.handle_message
            ))

    async def get_data(self):
        if self.last_values:
            symbol = next(iter(self.last_values.keys()))
            return [symbol, self.last_values.pop(symbol)]
        else:
            return None
