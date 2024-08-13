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

