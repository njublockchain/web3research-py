from typing import Any, Dict, Optional
from web3research.common.types import ChainStyle
from web3research.db import ClickhouseProvider
from web3research.common.type_convert import (
    convert_bytes_to_hex_generator,
    group_events_generator,
)
from web3research.eth.formats import (
    ETHEREUM_BLOCK_COLUMN_FORMATS,
    ETHEREUM_EVENT_COLUMN_FORMATS,
    ETHEREUM_TRACE_COLUMN_FORMATS,
    ETHEREUM_TRANSACTION_COLUMN_FORMATS,
)


class EthereumProvider(ClickhouseProvider):
    """EthereumProvider is a provider for fetching data on Ethereum from the backend."""

    def __init__(
        self,
        api_token,  # required
        backend: Optional[str] = None,
        database: str = "ethereum",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Create an EthereumProvider instance. This is a provider for data on Ethereum.

        Args:
            api_token (str): The Web3Research API token.
            backend (str, optional): The Web3Research backend to use. Defaults to None.
            database (str, optional): The database to use. Defaults to "ethereum".
            settings (Dict[str, Any], optional): The Clickhouse settings to use. Defaults to None.
            generic_args (Dict[str, Any], optional): The Clickhouse generic arguments to use. Defaults to None.
            **kwargs: Additional Clickhouse keyword arguments.
        Returns:
            EthereumProvider: An EthereumProvider instance.
        """
        super().__init__(
            api_token=api_token,
            backend=backend,
            database=database,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )
        self.database = database

    def blocks(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"number": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get blocks from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to number ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of blocks.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join([k + " " + "ASC" if v else "DESC" for k, v in order_by.items()])
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""
        q = """
        SELECT * 
        FROM {database}.blocks 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """.format(
            database=self.database,
            where_phrase=where_phrase,
            order_by_phrase=order_by_phrase,
            limit_phrase=limit_phrase,
            offset_phrase=offset_phrase,
        )

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_BLOCK_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.ETH, named_results)

    def transactions(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {
            "blockNumber": True,
            "transactionIndex": True,
        },
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get transactions from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNumber and transactionIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of transactions.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join([k + " " + "ASC" if v else "DESC" for k, v in order_by.items()])
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""
        q = """
        SELECT * 
        FROM {database}.transactions 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """.format(
            database=self.database,
            where_phrase=where_phrase,
            order_by_phrase=order_by_phrase,
            limit_phrase=limit_phrase,
            offset_phrase=offset_phrase,
        )

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_TRANSACTION_COLUMN_FORMATS,
            parameters={
                **(parameters or {}),
            },
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.ETH, named_results)

    def traces(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNumber": True, "blockPos": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get traces from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNumber and blockPos ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of traces.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join([k + " " + "ASC" if v else "DESC" for k, v in order_by.items()])
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""
        q = """
        SELECT * 
        FROM {database}.traces 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """.format(
            database=self.database,
            where_phrase=where_phrase,
            order_by_phrase=order_by_phrase,
            limit_phrase=limit_phrase,
            offset_phrase=offset_phrase,
        )

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_TRACE_COLUMN_FORMATS,
            parameters={
                **(parameters or {}),
            },
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.ETH, named_results)

    def events(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNumber": True, "transactionIndex": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get events from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNumber, transactionIndex, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of events.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join([k + " " + "ASC" if v else "DESC" for k, v in order_by.items()])
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""
        q = """
        SELECT * 
        FROM {database}.events 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """.format(
            database=self.database,
            where_phrase=where_phrase,
            order_by_phrase=order_by_phrase,
            limit_phrase=limit_phrase,
            offset_phrase=offset_phrase,
        )

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_EVENT_COLUMN_FORMATS,
            parameters={
                **(parameters or {}),
            },
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return group_events_generator(
                convert_bytes_to_hex_generator(ChainStyle.ETH, named_results)
            )
