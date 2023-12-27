from kylink.db import ClickhouseProvider
from pycoingecko import CoinGeckoAPI


# MarketProvider is a wrapper to the Coingecko Free API
class MarketProvider(CoinGeckoAPI):
    def __init__(self, raw_provider: ClickhouseProvider):
        self.raw_provider = raw_provider
        super().__init__()

