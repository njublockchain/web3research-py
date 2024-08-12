import base64
from io import BytesIO
import json
import os
from typing import Any, Dict, Optional
from web3research.eth import EthereumProvider


class Web3Research:
    def __init__(
        self,
        api_token: str,
    ) -> None:
        self.api_token = api_token

    def eth(
        self,
        backend: Optional[str] = None,
        database: str = "ethereum",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        return EthereumProvider(
            api_token=self.api_token,
            backend=backend,
            database=database,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )

    def ethereum(
        self,
        backend: Optional[str] = None,
        database: str = "ethereum",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        return EthereumProvider(
            api_token=self.api_token,
            backend=backend,
            database=database,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )

    def install(self):
        # Set this _before_ importing matplotlib
        os.environ["MPLBACKEND"] = "AGG"

    def plotly(self, plot):
        # Encode to a base64 str
        html = "data:w3r/html;base64," + base64.b64encode(plot.to_html()).decode(
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
        print("data:w3r/table;json," + json.dumps(table_element_list))

    def image(self, plt):
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        # Encode to a base64 str
        img = "data:image/png;base64," + base64.b64encode(buf.read()).decode("utf-8")
        # Write to stdout
        print(img)
        plt.clf()
