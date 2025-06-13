"""
Bitcoin data provider for querying Bitcoin blockchain data.

This module provides a high-level interface for querying Bitcoin blockchain data
from ClickHouse databases, similar to the Ethereum and TRON providers.
"""

from typing import Any, Dict, Generator, Optional
from web3research.db.provider import ClickhouseProvider
from web3research.btc.formats import (
    BITCOIN_BLOCK_COLUMN_FORMATS,
    BITCOIN_INPUT_COLUMN_FORMATS,
    BITCOIN_OUTPUT_COLUMN_FORMATS,
    QUERY_FORMATS,
)


class BitcoinProvider(ClickhouseProvider):
    """BitcoinProvider is a provider for fetching data on Bitcoin from the backend."""

    def __init__(
        self,
        api_token,  # required
        backend: Optional[str] = None,
        database: str = "bitcoin",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Create a BitcoinProvider instance. This is a provider for data on Bitcoin.

        Args:
            api_token (str): The Web3Research API token.
            backend (str, optional): The Web3Research backend to use. Defaults to None.
            database (str, optional): The database to use. Defaults to "bitcoin".
            settings (Dict[str, Any], optional): The Clickhouse settings to use. Defaults to None.
            generic_args (Dict[str, Any], optional): The Clickhouse generic arguments to use. Defaults to None.
            **kwargs: Additional Clickhouse keyword arguments.
        Returns:
            BitcoinProvider: A BitcoinProvider instance.
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
        where: Optional[str] = None,
        order_by: Optional[Dict[str, bool]] = {"height": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Get blocks from the database.

        Args:
            where (str, optional): The WHERE clause. Defaults to None.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to height ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of blocks.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join(
                [k + " " + ("ASC" if v else "DESC") for k, v in order_by.items()]
            )
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
            column_formats=BITCOIN_BLOCK_COLUMN_FORMATS,
            query_formats=QUERY_FORMATS,
            parameters={**(parameters or {})},
        )
        column_names: list[str] = rows_stream.source.column_names  # type: ignore

        with rows_stream:
            for row in rows_stream:
                yield dict(zip(column_names, row))

    def inputs(
        self,
        where: Optional[str] = None,
        order_by: Optional[Dict[str, bool]] = {
            "blockHeight": True,
            "txIndex": True,
            "index": True,
        },
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Get transaction inputs from the database.

        Args:
            where (str, optional): The WHERE clause. Defaults to None.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockHeight, txIndex and index ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of transaction inputs.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join(
                [k + " " + ("ASC" if v else "DESC") for k, v in order_by.items()]
            )
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""

        q = """
        SELECT * 
        FROM {database}.inputs 
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
            column_formats=BITCOIN_INPUT_COLUMN_FORMATS,
            query_formats=QUERY_FORMATS,
            parameters={**(parameters or {})},
        )
        column_names: list[str] = rows_stream.source.column_names  # type: ignore

        with rows_stream:
            for row in rows_stream:
                yield dict(zip(column_names, row))

    def outputs(
        self,
        where: Optional[str] = None,
        order_by: Optional[Dict[str, bool]] = {
            "blockHeight": True,
            "txIndex": True,
            "index": True,
        },
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Get transaction outputs from the database.

        Args:
            where (str, optional): The WHERE clause. Defaults to None.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockHeight, txIndex and index ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of transaction outputs.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join(
                [k + " " + ("ASC" if v else "DESC") for k, v in order_by.items()]
            )
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""

        q = """
        SELECT * 
        FROM {database}.outputs 
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
            column_formats=BITCOIN_OUTPUT_COLUMN_FORMATS,
            query_formats=QUERY_FORMATS,
            parameters={**(parameters or {})},
        )
        column_names: list[str] = rows_stream.source.column_names  # type: ignore

        with rows_stream:
            for row in rows_stream:
                yield dict(zip(column_names, row))

    def transactions(
        self,
        where: Optional[str] = None,
        order_by: Optional[Dict[str, bool]] = {
            "blockHeight": True,
            "txIndex": True,
        },
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Get unique transactions from the database by aggregating inputs.

        This method provides a view of Bitcoin transactions by selecting unique transaction
        information from the inputs table, which contains transaction-level data.

        Args:
            where (str, optional): The WHERE clause. Defaults to None.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockHeight and txIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of transactions.
        """
        where_phrase = "WHERE " + where if where else ""
        order_by_phrase = (
            "ORDER BY "
            + ", ".join(
                [k + " " + ("ASC" if v else "DESC") for k, v in order_by.items()]
            )
            if order_by
            else ""
        )
        limit_phrase = "LIMIT " + str(limit) if limit else ""
        offset_phrase = "OFFSET " + str(offset) if offset else ""

        q = """
        SELECT DISTINCT
            txid,
            txIndex,
            totalSize,
            baseSize,
            vsize,
            weight,
            version,
            lockTime,
            blockHash,
            blockHeight,
            blockTime
        FROM {database}.inputs 
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

        # Use a subset of input formats for transaction fields
        tx_formats = {
            k: v
            for k, v in BITCOIN_INPUT_COLUMN_FORMATS.items()
            if k
            in [
                "txid",
                "txIndex",
                "totalSize",
                "baseSize",
                "vsize",
                "weight",
                "version",
                "lockTime",
                "blockHash",
                "blockHeight",
                "blockTime",
            ]
        }

        rows_stream = self.query_rows_stream(
            q,
            column_formats=tx_formats,
            query_formats=QUERY_FORMATS,
            parameters={**(parameters or {})},
        )
        column_names: list[str] = rows_stream.source.column_names  # type: ignore

        with rows_stream:
            for row in rows_stream:
                yield dict(zip(column_names, row))
