import base64
from io import BytesIO
import json
import os
from kylink.eth import EthereumProvider


class Kylink:
    def __init__(self, api_token) -> None:
        self.eth = EthereumProvider(api_token)

    def install(self):
        # Set this _before_ importing matplotlib
        os.environ["MPLBACKEND"] = "AGG"

    def plotly(self, plot):
        # Encode to a base64 str
        html = "data:kylink/html;base64," + base64.b64encode(plot.to_html()).decode(
            "utf-8"
        )
        print(html)

    def matplotlib(self, plt):
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        # Encode to a base64 str
        img = "data:image/png;base64," + base64.b64encode(buf.read()).decode("utf-8")
        # Write to stdout
        print(img)
        plt.clf()

    def table(self, table_element_list):
        print("data:kylink/table;json," + json.dumps(table_element_list))

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
