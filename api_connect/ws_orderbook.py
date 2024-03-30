from ws_connect import ws
from time import sleep


def handle_message(message):
    print(message['data'])


ws.orderbook_stream(
    depth=200,
    symbol="ETHUSDT",
    callback=handle_message
)
while True:
    sleep(1)
