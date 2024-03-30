import os

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

load_dotenv()
SECRET = os.getenv('SECRET')
OPEN = os.getenv('OPEN')

session = HTTP(
    testnet=False,
    api_key=OPEN,
    api_secret=SECRET,
)
