from typing import Any, Dict, Optional, Union
from inspect import signature
from urllib.parse import urlparse, parse_qs

from clickhouse_connect.driver.httpclient import HttpClient
from clickhouse_connect.driver.exceptions import ProgrammingError


class ClickhouseProvider(HttpClient):
    def __init__(
        self,
        api_token: str,
        *,
        host: str = "db.kylink.xyz",
        database: Optional[str] = None,
        interface: Optional[str] = None,
        port: int = 443,
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self._api_token = api_token

        if api_token is None and "user" in kwargs:
            api_token = kwargs.pop("user")
        if api_token is None and "user_name" in kwargs:
            api_token = kwargs.pop("user_name")

        if "compression" in kwargs and "compress" not in kwargs:
            kwargs["compress"] = kwargs.pop("compression")

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

        super().__init__(
            "https",  # interface is always https
            host,
            port,
            api_token,  # username is api_token
            "",  # password is empty
            database,
            settings=settings,
            **kwargs,
        )
