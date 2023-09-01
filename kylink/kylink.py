import os
import json
import base64
from io import BytesIO

from kylink.defi import DeFiProvider
from kylink.market import MarketProvider
from kylink.gql import GraphQLProvider
from kylink.resolve import ResolveProvider
from kylink.token import TokenProvider
from kylink.wallet import WalletProvider

class Kylink:
    def __init__(self, api=None) -> None:
        self.raw = GraphQLProvider(api)
        self.market = MarketProvider(self.raw)
        self.defi = DeFiProvider(self.raw)
        self.resolve = ResolveProvider(self.raw)
        self.wallet = WalletProvider(self.raw)
        self.token = TokenProvider(self.raw)

    def install(self):
        # Set this _before_ importing matplotlib
        os.environ['MPLBACKEND'] = 'AGG'

    def image(self, plt):
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        # Encode to a base64 str
        img = "data:image/png;base64," + base64.b64encode(buf.read()).decode("utf-8")
        # Write to stdout
        print(img)
        plt.clf()

    def table(self, table_element_list):
        print("data:table/" + json.dumps(table_element_list))
