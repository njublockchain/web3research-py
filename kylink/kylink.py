import os
import base64
from io import BytesIO

from kylink.defi import DeFiProvider
from kylink.market import MarketProvider
from kylink.gql import GraphQLProvider
from kylink.resolve import ResolveProvider
from kylink.token import TokenProvider
from kylink.wallet import WalletProvider

import matplotlib.pyplot as plt


# Patch
def ensure_matplotlib_patch():
    # _old_show = plt.show

    def show():
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        # Encode to a base64 str
        img = "data:image/png;base64," + base64.b64encode(buf.read()).decode("utf-8")
        # Write to stdout
        print(img)
        plt.clf()

    plt.show = show


os.environ["MPLBACKEND"] = "AGG"

ensure_matplotlib_patch()


class Kylink:
    def __init__(self, api=None) -> None:
        self.raw = GraphQLProvider(api)
        self.market = MarketProvider(self.raw)
        self.defi = DeFiProvider(self.raw)
        self.resolve = ResolveProvider(self.raw)
        self.wallet = WalletProvider(self.raw)
        self.token = TokenProvider(self.raw)
        self.utils = Utils()

    def install(self):
        ensure_matplotlib_patch()

    def table(self, table_element_list):
        print("data:table/" + json.dumps(table_element_list))
