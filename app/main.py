import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import redis
from .schedulers import (
    update_volume_scheduler,
    check_anomaly_volume_scheduler,
    check_rsi_scheduler)
from redis_utils.consumers import signal_consumer, create_xgroup
from redis_utils.proscessing import TimeSeries
from stream.ws_stream import launch_stream
from data.get_data import Klines
from telegram_bot.chat import SignalMessage
from data.analytics import Analytics


async def create_consumer_xgroups():
    await create_xgroup('volume')
    await create_xgroup('rsi')


async def load_historical_klines_5_to_redis(symbol_list: list):
    klines = Klines(symbol_list, 5)
    await asyncio.create_task(klines.first_launch())


async def load_historical_klines_30_to_dataframes(symbol_list: list):
    klines = Klines(symbol_list, 30, 24)
    await asyncio.create_task(klines.first_launch())


@asynccontextmanager
async def lifespan(app: FastAPI):
    symbol_list = ['10000LADYSUSDT', '10000NFTUSDT', '10000SATSUSDT', '10000STARLUSDT', '10000WENUSDT', '1000BONKUSDT', '1000BTTUSDT', '1000FLOKIUSDT', '1000LUNCUSDT', '1000PEPEUSDT', '1000RATSUSDT', '1000TURBOUSDT', '1000XECUSDT', '1CATUSDT', '1INCHUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ADAUSDT', 'AERGOUSDT', 'AGIUSDT', 'AGIXUSDT', 'AGLDUSDT', 'AIUSDT', 'AKROUSDT', 'ALGOUSDT', 'ALICEUSDT', 'ALPACAUSDT', 'ALPHAUSDT', 'ALTUSDT', 'AMBUSDT', 'ANKRUSDT', 'ANTUSDT', 'APEUSDT', 'API3USDT', 'APTUSDT', 'ARBUSDT', 'ARKMUSDT', 'ARKUSDT', 'ARPAUSDT', 'ARUSDT', 'ASTRUSDT', 'ATAUSDT', 'ATOMUSDT', 'AUCTIONUSDT', 'AUDIOUSDT', 'AVAXUSDT', 'AXLUSDT', 'AXSUSDT', 'BADGERUSDT', 'BAKEUSDT', 'BALUSDT', 'BANDUSDT', 'BATUSDT', 'BCHUSDT', 'BEAMUSDT', 'BELUSDT', 'BICOUSDT', 'BIGTIMEUSDT', 'BLURUSDT', 'BLZUSDT', 'BNBPERP', 'BNBUSDT', 'BNTUSDT', 'BNXUSDT', 'BOBAUSDT', 'BONDUSDT', 'BSVUSDT', 'BSWUSDT', 'BTCPERP', 'BTCUSDT', 'C98USDT', 'CAKEUSDT', 'CEEKUSDT', 'CELOUSDT', 'CELRUSDT', 'CETUSUSDT', 'CFXUSDT', 'CHRUSDT', 'CHZUSDT', 'CKBUSDT', 'COMBOUSDT', 'COMPUSDT', 'COREUSDT', 'COTIUSDT', 'CROUSDT', 'CRVUSDT', 'CTCUSDT', 'CTKUSDT', 'CTSIUSDT', 'CVCUSDT', 'CVXUSDT', 'CYBERUSDT', 'DARUSDT', 'DASHUSDT', 'DATAUSDT', 'DENTUSDT', 'DGBUSDT', 'DODOUSDT', 'DOGEUSDT', 'DOTUSDT', 'DUSKUSDT', 'DYDXUSDT', 'DYMUSDT', 'EDUUSDT', 'EGLDUSDT', 'ENJUSDT', 'ENSUSDT', 'EOSUSDT', 'ETCPERP', 'ETCUSDT', 'ETHPERP', 'ETHUSDT', 'ETHWUSDT', 'FETUSDT', 'FILUSDT', 'FITFIUSDT', 'FLMUSDT', 'FLOWUSDT', 'FLRUSDT', 'FORTHUSDT', 'FRONTUSDT', 'FTMUSDT', 'FUNUSDT', 'FXSUSDT', 'GALAUSDT', 'GALUSDT', 'GASUSDT', 'GFTUSDT', 'GLMRUSDT', 'GLMUSDT', 'GMTUSDT', 'GMXUSDT', 'GODSUSDT', 'GPTUSDT', 'GRTUSDT', 'GTCUSDT', 'HBARUSDT', 'HFTUSDT', 'HIFIUSDT', 'HIGHUSDT', 'HNTUSDT', 'HOOKUSDT', 'HOTUSDT', 'ICPUSDT', 'ICXUSDT', 'IDEXUSDT', 'IDUSDT', 'ILVUSDT', 'IMXUSDT', 'INJUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IOTXUSDT', 'JASMYUSDT', 'JOEUSDT', 'JSTUSDT', 'JTOUSDT', 'JUPUSDT', 'KASUSDT', 'KAVAUSDT', 'KDAUSDT', 'KEYUSDT', 'KLAYUSDT', 'KNCUSDT', 'KSMUSDT', 'LDOUSDT', 'LEVERUSDT', 'LINAUSDT', 'LINKUSDT', 'LITUSDT', 'LOOKSUSDT', 'LOOMUSDT', 'LPTUSDT', 'LQTYUSDT', 'LRCUSDT', 'LSKUSDT', 'LTCUSDT', 'LUNA2USDT', 'MAGICUSDT', 'MANAUSDT', 'MANTAUSDT', 'MASKUSDT', 'MATICPERP', 'MATICUSDT', 'MAVIAUSDT', 'MAVUSDT', 'MBLUSDT', 'MBOXUSDT', 'MDTUSDT', 'MEMEUSDT', 'METISUSDT', 'MINAUSDT', 'MKRUSDT', 'MNTUSDT', 'MOBILEUSDT', 'MOVRUSDT', 'MTLUSDT', 'MULTIUSDT', 'MYRIAUSDT', 'MYROUSDT', 'NEARUSDT', 'NEOUSDT', 'NFPUSDT', 'NKNUSDT', 'NMRUSDT', 'NTRNUSDT', 'OCEANUSDT', 'OGNUSDT', 'OGUSDT', 'OMGUSDT', 'ONDOUSDT', 'ONEUSDT', 'ONGUSDT', 'ONTUSDT', 'OPPERP', 'OPUSDT', 'ORBSUSDT', 'ORDIUSDT', 'OXTUSDT', 'PAXGUSDT', 'PENDLEUSDT', 'PEOPLEUSDT', 'PERPUSDT', 'PHBUSDT', 'PIXELUSDT', 'POLYXUSDT', 'PORTALUSDT', 'POWRUSDT', 'PROMUSDT', 'PYTHUSDT', 'QIUSDT', 'QNTUSDT', 'QTUMUSDT', 'RADUSDT', 'RAREUSDT', 'RDNTUSDT', 'REEFUSDT', 'RENUSDT', 'REQUSDT', 'RIFUSDT', 'RLCUSDT', 'RNDRUSDT', 'RONUSDT', 'ROSEUSDT', 'RPLUSDT', 'RSRUSDT', 'RSS3USDT', 'RUNEUSDT', 'RVNUSDT', 'SANDUSDT', 'SCRTUSDT', 'SCUSDT', 'SEIUSDT', 'SFPUSDT', 'SHIB1000USDT', 'SILLYUSDT', 'SKLUSDT', 'SLPUSDT', 'SNTUSDT', 'SNXUSDT', 'SOLPERP', 'SOLUSDT', 'SPELLUSDT', 'SSVUSDT', 'STEEMUSDT', 'STGUSDT', 'STMXUSDT', 'STORJUSDT', 'STPTUSDT', 'STRAXUSDT', 'STRKUSDT', 'STXUSDT', 'SUIUSDT', 'SUNUSDT', 'SUPERUSDT', 'SUSHIUSDT', 'SWEATUSDT', 'SXPUSDT', 'TAOUSDT', 'THETAUSDT', 'TIAUSDT', 'TLMUSDT', 'TOKENUSDT', 'TOMIUSDT', 'TONUSDT', 'TRBUSDT', 'TRUUSDT', 'TRXUSDT', 'TUSDT', 'TWTUSDT', 'UMAUSDT', 'UNFIUSDT', 'UNIUSDT', 'USDCUSDT', 'USTCUSDT', 'VETUSDT', 'VGXUSDT', 'VRAUSDT', 'VTHOUSDT', 'WAVESUSDT', 'WAXPUSDT', 'WIFUSDT', 'WLDUSDT', 'WOOUSDT', 'XAIUSDT', 'XCNUSDT', 'XEMUSDT', 'XLMUSDT', 'XMRUSDT', 'XNOUSDT', 'XRDUSDT', 'XRPPERP', 'XRPUSDT', 'XTZUSDT', 'XVGUSDT', 'XVSUSDT', 'YFIIUSDT', 'YFIUSDT', 'YGGUSDT', 'ZECUSDT', 'ZENUSDT', 'ZETAUSDT', 'ZILUSDT', 'ZKFUSDT', 'ZRXUSDT']
    #symbol_list = ['BTCUSDT'] #, 'BTCUSDT', 'SOLUSDT'

    first_launch_tasks = await asyncio.gather(
        #load_historical_klines_5_to_redis(symbol_list),
        create_consumer_xgroups()
    )

    kline_stream = asyncio.create_task(launch_stream(symbol_list))

    volume_updater = asyncio.create_task(update_volume_scheduler(symbol_list))
    volume_checker = asyncio.create_task(check_anomaly_volume_scheduler(symbol_list))
    rsi_checker = asyncio.create_task(check_rsi_scheduler(symbol_list))
    #volume_cons = asyncio.create_task(signal_consumer('volume'))

    yield
    print('End')

app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/media", StaticFiles(directory="media"), name="media")
