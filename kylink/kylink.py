import base64
from io import BytesIO
import json
import os
from kylink.eth import EthereumProvider


class Kylink:
    def __init__(self, api=None) -> None:
        self.eth = EthereumProvider(api)

    def install(self):
        # Set this _before_ importing matplotlib
        os.environ["MPLBACKEND"] = "AGG"

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
