import asyncio

from api_connect.ws_kline import WSKline
from redis_utils.proscessing import TimeSeries


class StartStream:
    def __init__(self, symbol) -> None:
        self.symbol: list = symbol
        self.ws_kline = WSKline(self.symbol)
        self.redis_ts = TimeSeries()

    async def stream(self):
        '''Запуск стрима'''
        await self.ws_kline.stream()
        #await self.ws_kline.check_condition()

    async def cycle(self):
        '''Шедулер стрима'''
        while True:
            data = await self.ws_kline.get_data()
            if data:
                asyncio.create_task(self.add_volume(data))
            await asyncio.sleep(0.1)

    async def extract_topic(self, topic):
        if topic:
            coin = topic.split('.')[-1]
            return coin

    async def add_volume(self, data):
        # 'data' = ('kline.5.10000LADYSUSDT', [{'start': 1711759800000, 'end': 1711760099999, 'interval': '5', 'open': '0.0025689', 'close': '0.0025733', 'high': '0.0025767', 'low': '0.0025676', 'volume': '3577200', 'turnover': '9203.29365', 'confirm': False, 'timestamp': 1711759969798}])
        if data:
            coin = await self.extract_topic(data[0])
            print(coin)
            timestamp_end = data[1][0]['end']
            volume = data[1][0]['volume']
            #print(f'{coin} {timestamp_end} {volume}')
            await self.redis_ts.create_timeseries(coin, 'volume')
            await self.redis_ts.add_timeseries(coin, 'volume', timestamp_end, volume)
        else:
            print('WSKLINE FAILED')


#    async def add_volume(self, data):
#        if data:
#            coin = data['topic'].split('.')[-1]
#            timestamp_end = data['data'][0]['end']
#            volume = data['data'][0]['volume']
#            #print(f'{coin} {timestamp_end} {volume}')
#            await self.redis_ts.create_timeseries(coin, 'volume')
#            await self.redis_ts.add_timeseries(coin, 'volume', timestamp_end, volume)
#        else:
#            print('WSKLINE FAILED')


    async def starter(self):
        '''Старт стрима в рамках объекта'''
        stream_starter = asyncio.create_task(self.stream())
        print(stream_starter)
        await self.cycle()


async def launch_stream(symbol_list):
    #symbol_list = ['10000LADYSUSDT', '10000NFTUSDT', '10000SATSUSDT', '10000STARLUSDT', '10000WENUSDT', '1000BONKUSDT', '1000BTTUSDT', '1000FLOKIUSDT', '1000LUNCUSDT', '1000PEPEUSDT', '1000RATSUSDT', '1000TURBOUSDT', '1000XECUSDT', '1CATUSDT', '1INCHUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ADAUSDT', 'AERGOUSDT', 'AGIUSDT', 'AGIXUSDT', 'AGLDUSDT', 'AIUSDT', 'AKROUSDT', 'ALGOUSDT', 'ALICEUSDT', 'ALPACAUSDT', 'ALPHAUSDT', 'ALTUSDT', 'AMBUSDT', 'ANKRUSDT', 'ANTUSDT', 'APEUSDT', 'API3USDT', 'APTUSDT', 'ARBUSDT', 'ARKMUSDT', 'ARKUSDT', 'ARPAUSDT', 'ARUSDT', 'ASTRUSDT', 'ATAUSDT', 'ATOMUSDT', 'AUCTIONUSDT', 'AUDIOUSDT', 'AVAXUSDT', 'AXLUSDT', 'AXSUSDT', 'BADGERUSDT', 'BAKEUSDT', 'BALUSDT', 'BANDUSDT', 'BATUSDT', 'BCHUSDT', 'BEAMUSDT', 'BELUSDT', 'BICOUSDT', 'BIGTIMEUSDT', 'BLURUSDT', 'BLZUSDT', 'BNBPERP', 'BNBUSDT', 'BNTUSDT', 'BNXUSDT', 'BOBAUSDT', 'BONDUSDT', 'BSVUSDT', 'BSWUSDT', 'BTC-08MAR24', 'BTC-15MAR24', 'BTC-22MAR24', 'BTC-26APR24', 'BTC-27SEP24', 'BTC-28JUN24', 'BTC-29MAR24', 'BTCPERP', 'BTCUSDT', 'C98USDT', 'CAKEUSDT', 'CEEKUSDT', 'CELOUSDT', 'CELRUSDT', 'CETUSUSDT', 'CFXUSDT', 'CHRUSDT', 'CHZUSDT', 'CKBUSDT', 'COMBOUSDT', 'COMPUSDT', 'COREUSDT', 'COTIUSDT', 'CROUSDT', 'CRVUSDT', 'CTCUSDT', 'CTKUSDT', 'CTSIUSDT', 'CVCUSDT', 'CVXUSDT', 'CYBERUSDT', 'DARUSDT', 'DASHUSDT', 'DATAUSDT', 'DENTUSDT', 'DGBUSDT', 'DODOUSDT', 'DOGEUSDT', 'DOTUSDT', 'DUSKUSDT', 'DYDXUSDT', 'DYMUSDT', 'EDUUSDT', 'EGLDUSDT', 'ENJUSDT', 'ENSUSDT', 'EOSUSDT', 'ETCPERP', 'ETCUSDT', 'ETH-08MAR24', 'ETH-15MAR24', 'ETH-22MAR24', 'ETH-26APR24', 'ETH-27SEP24', 'ETH-28JUN24', 'ETH-29MAR24', 'ETHPERP', 'ETHUSDT', 'ETHWUSDT', 'FETUSDT', 'FILUSDT', 'FITFIUSDT', 'FLMUSDT', 'FLOWUSDT', 'FLRUSDT', 'FORTHUSDT', 'FRONTUSDT', 'FTMUSDT', 'FUNUSDT', 'FXSUSDT', 'GALAUSDT', 'GALUSDT', 'GASUSDT', 'GFTUSDT', 'GLMRUSDT', 'GLMUSDT', 'GMTUSDT', 'GMXUSDT', 'GODSUSDT', 'GPTUSDT', 'GRTUSDT', 'GTCUSDT', 'HBARUSDT', 'HFTUSDT', 'HIFIUSDT', 'HIGHUSDT', 'HNTUSDT', 'HOOKUSDT', 'HOTUSDT', 'ICPUSDT', 'ICXUSDT', 'IDEXUSDT', 'IDUSDT', 'ILVUSDT', 'IMXUSDT', 'INJUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IOTXUSDT', 'JASMYUSDT', 'JOEUSDT', 'JSTUSDT', 'JTOUSDT', 'JUPUSDT', 'KASUSDT', 'KAVAUSDT', 'KDAUSDT', 'KEYUSDT', 'KLAYUSDT', 'KNCUSDT', 'KSMUSDT', 'LDOUSDT', 'LEVERUSDT', 'LINAUSDT', 'LINKUSDT', 'LITUSDT', 'LOOKSUSDT', 'LOOMUSDT', 'LPTUSDT', 'LQTYUSDT', 'LRCUSDT', 'LSKUSDT', 'LTCUSDT', 'LUNA2USDT', 'MAGICUSDT', 'MANAUSDT', 'MANTAUSDT', 'MASKUSDT', 'MATICPERP', 'MATICUSDT', 'MAVIAUSDT', 'MAVUSDT', 'MBLUSDT', 'MBOXUSDT', 'MDTUSDT', 'MEMEUSDT', 'METISUSDT', 'MINAUSDT', 'MKRUSDT', 'MNTUSDT', 'MOBILEUSDT', 'MOVRUSDT', 'MTLUSDT', 'MULTIUSDT', 'MYRIAUSDT', 'MYROUSDT', 'NEARUSDT', 'NEOUSDT', 'NFPUSDT', 'NKNUSDT', 'NMRUSDT', 'NTRNUSDT', 'OCEANUSDT', 'OGNUSDT', 'OGUSDT', 'OMGUSDT', 'ONDOUSDT', 'ONEUSDT', 'ONGUSDT', 'ONTUSDT', 'OPPERP', 'OPUSDT', 'ORBSUSDT', 'ORDIUSDT', 'OXTUSDT', 'PAXGUSDT', 'PENDLEUSDT', 'PEOPLEUSDT', 'PERPUSDT', 'PHBUSDT', 'PIXELUSDT', 'POLYXUSDT', 'PORTALUSDT', 'POWRUSDT', 'PROMUSDT', 'PYTHUSDT', 'QIUSDT', 'QNTUSDT', 'QTUMUSDT', 'RADUSDT', 'RAREUSDT', 'RDNTUSDT', 'REEFUSDT', 'RENUSDT', 'REQUSDT', 'RIFUSDT', 'RLCUSDT', 'RNDRUSDT', 'RONUSDT', 'ROSEUSDT', 'RPLUSDT', 'RSRUSDT', 'RSS3USDT', 'RUNEUSDT', 'RVNUSDT', 'SANDUSDT', 'SCRTUSDT', 'SCUSDT', 'SEIUSDT', 'SFPUSDT', 'SHIB1000USDT', 'SILLYUSDT', 'SKLUSDT', 'SLPUSDT', 'SNTUSDT', 'SNXUSDT', 'SOLPERP', 'SOLUSDT', 'SPELLUSDT', 'SSVUSDT', 'STEEMUSDT', 'STGUSDT', 'STMXUSDT', 'STORJUSDT', 'STPTUSDT', 'STRAXUSDT', 'STRKUSDT', 'STXUSDT', 'SUIUSDT', 'SUNUSDT', 'SUPERUSDT', 'SUSHIUSDT', 'SWEATUSDT', 'SXPUSDT', 'TAOUSDT', 'THETAUSDT', 'TIAUSDT', 'TLMUSDT', 'TOKENUSDT', 'TOMIUSDT', 'TONUSDT', 'TRBUSDT', 'TRUUSDT', 'TRXUSDT', 'TUSDT', 'TWTUSDT', 'UMAUSDT', 'UNFIUSDT', 'UNIUSDT', 'USDCUSDT', 'USTCUSDT', 'VETUSDT', 'VGXUSDT', 'VRAUSDT', 'VTHOUSDT', 'WAVESUSDT', 'WAXPUSDT', 'WIFUSDT', 'WLDUSDT', 'WOOUSDT', 'XAIUSDT', 'XCNUSDT', 'XEMUSDT', 'XLMUSDT', 'XMRUSDT', 'XNOUSDT', 'XRDUSDT', 'XRPPERP', 'XRPUSDT', 'XTZUSDT', 'XVGUSDT', 'XVSUSDT', 'YFIIUSDT', 'YFIUSDT', 'YGGUSDT', 'ZECUSDT', 'ZENUSDT', 'ZETAUSDT', 'ZILUSDT', 'ZKFUSDT', 'ZRXUSDT']
    #symbol_list = ['BTCUSDT', 'ETHUSDT', 'DOGEUSDT',]
    start = StartStream(symbol_list)
    await start.starter()
