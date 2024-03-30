from .ws_connect import ws


class WSTrade:
    def __init__(self, symbol) -> None:
        self.data = []
        self.pereodic_data = []
        self.symbol = symbol
        self.message = None

    def handle_message(self, message):
        self.data.append(message['data'][0])
        self.pereodic_data.append(message['data'][0])
        self.message = message

    async def stream(self):
        ws.trade_stream(
            symbol=self.symbol,
            callback=self.handle_message
        )

    def get_data(self):
        if self.data:
            return self.data

    def get_message(self):
        return self.message

    def update_pereodic_data(self, method):
        if method == 'clear':
            self.pereodic_data.clear()
        elif method == 'get':
            return self.pereodic_data
