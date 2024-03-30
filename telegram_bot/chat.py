import asyncio
import os

from dotenv import load_dotenv

import telegram

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TOKEN')
TELEGRAM_CHAT_ID = os.getenv('CHAT_ID')
GROUP_CHAT_ID = os.getenv('GROUP_CHAT_ID')
bot = telegram.Bot(token=TELEGRAM_TOKEN)

class SignalMessage:
    def __init__(
            self,
            symbol: str,
            indicator: str,
            img_id: str,
            rsi: str = ''  # 'overbought' / oversold
            ) -> None:
        self.symbol = symbol
        self.indicator = indicator
        self.img_id = img_id
        self.rsi = rsi
        self.message = ''

    async def validate(self):
        self.message += self.symbol
        if self.indicator == 'volume':
            self.message += '. Аномальный объем!'
        if self.rsi == 'oversold':
            self.message += ' Перепроданность по RSI'
        elif self.rsi == 'overbought':
            self.message += ' Перекупленность по RSI'

    async def send(self):
        #await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text='message')
        print(f'media/signals/{self.symbol}_{self.img_id}.png')
        await self.validate()
        await bot.send_photo(
           chat_id=TELEGRAM_CHAT_ID,
           photo=f'media/signals/{self.symbol}_{self.img_id}.png',
           caption=self.message)

    async def send_to_group(self):
        await self.validate()
        #await bot.send_message(chat_id=GROUP_CHAT_ID, text=self.message)
        print(f'media/signals/{self.symbol}_{self.img_id}.png')
        img_path = f'media/signals/{self.symbol}_{self.img_id}.png'
        try:
            await bot.send_photo(
               chat_id=GROUP_CHAT_ID,
               photo=open(img_path, 'rb'),
               caption=self.message)
        except telegram.error.RetryAfter as error:
            print(print(f'{error.retry_after=}'))
            await asyncio.sleep(int(error.retry_after)+1)
            print('Seconds:', error.retry_after)

