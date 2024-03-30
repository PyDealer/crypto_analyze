from ws_connect import ws
from time import sleep


def handle_message(message):
    print(
        message['data'])


ws.ticker_stream(
    symbol="ETHUSDT",
    callback=handle_message
)
while True:
    sleep(1)
