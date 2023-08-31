from kylink.defi import DeFiProvider
from kylink.market import MarketProvider
from kylink.gql import GraphQLProvider
from kylink.resolve import ResolveProvider
from kylink.token import TokenProvider
from kylink.wallet import WalletProvider
from kylink.utils import Utils


class Kylink:
    def __init__(self, api=None) -> None:
        self.raw = GraphQLProvider(api)
        self.market = MarketProvider(self.raw)
        self.defi = DeFiProvider(self.raw)
        self.resolve = ResolveProvider(self.raw)
        self.wallet = WalletProvider(self.raw)
        self.token = TokenProvider(self.raw)
        self.utils = Utils()
