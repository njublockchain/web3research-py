from typing import Any, Dict, Optional
from web3research.eth import EthereumProvider
from web3research.tron.provider import TronProvider


class Web3Research:
    """Web3Research is the main entry point."""

    def __init__(
        self,
        api_token: str,
    ) -> None:
        """Create a Web3Research instance.

        Args:
            api_token (str): The Web3Research API token.
        """
        self.api_token = api_token

    def eth(
        self,
        backend: Optional[str] = None,
        database: str = "ethereum",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Create an EthereumProvider instance. This is a convenience method for creating an EthereumProvider instance.

        Args:
            backend (str, optional): The Web3Research backend to use. Defaults to None.
            database (str, optional): The database to use. Defaults to "ethereum".
            settings (Dict[str, Any], optional): The Clickhouse settings to use. Defaults to None.
            generic_args (Dict[str, Any], optional): The Clickhouse generic arguments to use. Defaults to None.
            **kwargs: Additional Clickhouse keyword arguments.
        Returns:
            EthereumProvider: An EthereumProvider instance.
        """
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
        """Create an EthereumProvider instance. Same as eth().

        Args:
            backend (str, optional): The Web3Research backend to use. Defaults to None.
            database (str, optional): The database to use. Defaults to "ethereum".
            settings (Dict[str, Any], optional): The Clickhouse settings to use. Defaults to None.
            generic_args (Dict[str, Any], optional): The Clickhouse generic arguments to use. Defaults to None.
            **kwargs: Additional Clickhouse keyword arguments.
        Returns:
            EthereumProvider: An EthereumProvider instance.
        """
        return EthereumProvider(
            api_token=self.api_token,
            backend=backend,
            database=database,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )

    def tron(
        self,
        backend: Optional[str] = None,
        database: str = "tron",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Create a TronProvider instance. This is a convenience method for creating a TronProvider instance.

        Args:
            backend (str, optional): The Web3Research backend to use. Defaults to None.
            database (str, optional): The database to use. Defaults to "tron".
            settings (Dict[str, Any], optional): The Clickhouse settings to use. Defaults to None.
            generic_args (Dict[str, Any], optional): The Clickhouse generic arguments to use. Defaults to None.
            **kwargs: Additional Clickhouse keyword arguments.
        Returns:
            TronProvider: A TronProvider instance.
        """
        return TronProvider(
            api_token=self.api_token,
            backend=backend,
            database=database,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )