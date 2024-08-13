import os
import logging
from typing import Any, Dict, Optional
from inspect import signature
from urllib.parse import urlparse

from clickhouse_connect.driver.httpclient import HttpClient
from clickhouse_connect.driver.exceptions import ProgrammingError

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "WARNING").upper())

logger = logging.getLogger(__name__)

DEFAULT_BACKEND_URIS = [
    "https://s1.web3resear.ch:443",
    "https://s2.web3resear.ch:443",
    "https://cn-s1.web3resear.ch:19443",
    "https://cn-s2.web3resear.ch:19443",
]


class ClickhouseProvider(HttpClient):
    """ClickhouseProvider is a wrapper around the Clickhouse HTTP client."""

    def __init__(
        self,
        api_token: str,
        database: str,
        *,
        backend: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self._api_token = api_token

        if api_token is None and "user" in kwargs:
            api_token = kwargs.pop("user")
        if api_token is None and "user_name" in kwargs:
            api_token = kwargs.pop("user_name")

        settings = settings or {}

        if api_token is None:
            raise ProgrammingError("api_token is required")
        if database is None:
            raise ProgrammingError("database is required")

        if generic_args:
            client_params = signature(HttpClient).parameters
            for name, value in generic_args.items():
                if name in client_params:
                    kwargs[name] = value
                elif name == "compression":
                    if "compress" not in kwargs:
                        kwargs["compress"] = value
                else:
                    if name.startswith("ch_"):
                        name = name[3:]
                    settings[name] = value

        if backend is None:
            for backend_uri_str in DEFAULT_BACKEND_URIS:
                backend_uri = urlparse(backend_uri_str)

                if backend_uri.hostname is None:
                    continue
                if backend_uri.port is None:
                    continue

                try:
                    super().__init__(
                        interface=backend_uri.scheme or "https",
                        host=backend_uri.hostname,
                        port=backend_uri.port,
                        username=api_token,  # username is api_token
                        password="",  # password is empty
                        database=database,
                        settings=settings,
                        **kwargs,
                    )
                    break
                except Exception as e:
                    logger.debug(
                        "Failed to connect to {backend_uri_str}: {error}".format(
                            backend_uri_str=backend_uri_str,
                            error=str(e),
                        )
                    )
        else:
            backend_uri = urlparse(backend)
            assert backend_uri.scheme in ["http", "https"]
            if backend_uri.hostname is None:
                raise ValueError("Invalid backend: {backend}".format(backend=backend))

            super().__init__(
                interface=backend_uri.scheme or "https",
                host=backend_uri.hostname,
                port=backend_uri.port or 443,
                username=api_token,  # username is api_token
                password="",  # password is empty
                database=database,
                settings=settings,
                **kwargs,
            )
