from typing import Any, Dict, Optional
from web3research.common.type_convert import (
    convert_bytes_to_hex_generator,
    group_events_generator,
)
from web3research.common.types import ChainStyle
from web3research.db import ClickhouseProvider
from web3research.tron.formats import (
    TRON_ACCOUNT_CREATE_CONTRACT_COLUMN_FORMATS,
    TRON_ACCOUNT_PERMISSION_UPDATE_CONTRACT_COLUMN_FORMATS,
    TRON_ACCOUNT_UPDATE_CONTRACT_COLUMN_FORMATS,
    TRON_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS,
    TRON_BLOCK_COLUMN_FORMATS,
    TRON_CANCEL_ALL_UNFREEZE_V2_CONTRACT_COLUMN_FORMATS,
    TRON_CLEAR_ABI_CONTRACT_COLUMN_FORMATS,
    TRON_CREATE_SMART_CONTRACT_COLUMN_FORMATS,
    TRON_DELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS,
    TRON_EVENT_COLUMN_FORMATS,
    TRON_EXCHANGE_CREATE_CONTRACT_COLUMN_FORMATS,
    TRON_EXCHANGE_INJECT_CONTRACT_COLUMN_FORMATS,
    TRON_EXCHANGE_TRANSACTION_CONTRACT_COLUMN_FORMATS,
    TRON_EXCHANGE_WITHDRAW_CONTRACT_COLUMN_FORMATS,
    TRON_FREEZE_BALANCE_CONTRACT_COLUMN_FORMATS,
    TRON_FREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS,
    TRON_INTERNAL_COLUMN_FORMATS,
    TRON_MARKET_CANCEL_ORDER_CONTRACT_COLUMN_FORMATS,
    TRON_MARKET_SELL_ASSET_CONTRACT_COLUMN_FORMATS,
    TRON_PARTICIPATE_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS,
    TRON_PROPOSAL_APPROVE_CONTRACT_COLUMN_FORMATS,
    TRON_PROPOSAL_CREATE_CONTRACT_COLUMN_FORMATS,
    TRON_PROPOSAL_DELETE_CONTRACT_COLUMN_FORMATS,
    TRON_SET_ACCOUNT_ID_CONTRACT_COLUMN_FORMATS,
    TRON_SHIELDED_TRANSFER_CONTRACT_COLUMN_FORMATS,
    TRON_TRANSACTION_COLUMN_FORMATS,
    TRON_TRANSFER_ASSET_CONTRACT_COLUMN_FORMATS,
    TRON_TRANSFER_CONTRACT_COLUMN_FORMATS,
    TRON_TRIGGER_SMART_CONTRACT_COLUMN_FORMATS,
    TRON_UNDELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS,
    TRON_UNFREEZE_ASSET_CONTRACT_COLUMN_FORMATS,
    TRON_UNFREEZE_BALANCE_CONTRACT_COLUMN_FORMATS,
    TRON_UNFREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS,
    TRON_UPDATE_ASSET_CONTRACT_COLUMN_FORMATS,
    TRON_UPDATE_BROKERAGE_CONTRACT_COLUMN_FORMATS,
    TRON_UPDATE_ENERGY_LIMIT_CONTRACT_COLUMN_FORMATS,
    TRON_UPDATE_SETTING_CONTRACT_COLUMN_FORMATS,
    TRON_VOTE_ASSET_CONTRACT_COLUMN_FORMATS,
    TRON_VOTE_WITNESS_CONTRACT_COLUMN_FORMATS,
    TRON_WITHDRAW_BALANCE_CONTRACT_COLUMN_FORMATS,
    TRON_WITHDRAW_EXPIRE_UNFREEZE_CONTRACT_COLUMN_FORMATS,
    TRON_WITNESS_CREATE_CONTRACT_COLUMN_FORMATS,
    TRON_WITNESS_UPDATE_CONTRACT_COLUMN_FORMATS,
)


class TronProvider(ClickhouseProvider):
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
            column_formats=TRON_BLOCK_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def transactions(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "index": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get transactions from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum and index ascending.
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
            column_formats=TRON_TRANSACTION_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def internals(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "internalIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get internals from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum and internalIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of internals.
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
        FROM {database}.internals 
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
            column_formats=TRON_INTERNAL_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def events(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get events from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
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
            column_formats=TRON_EVENT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return group_events_generator(
                convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)
            )

    def account_create_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get accountCreateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of accountCreateContracts.
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
        FROM {database}.accountCreateContracts 
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
            column_formats=TRON_ACCOUNT_CREATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def account_permission_update_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get accountPermissionUpdateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of accountPermissionUpdateContracts.
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
        FROM {database}.accountPermissionUpdateContracts 
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
            column_formats=TRON_ACCOUNT_PERMISSION_UPDATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def account_update_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get accountUpdateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of accountUpdateContracts.
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
        FROM {database}.accountUpdateContracts 
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
            column_formats=TRON_ACCOUNT_UPDATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def asset_issue_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get assetIssueContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of assetIssueContracts.
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
        FROM {database}.assetIssueContracts 
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
            column_formats=TRON_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def cancel_all_unfreeze_v2_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get cancelAllUnfreezeV2Contracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of cancelAllUnfreezeV2Contracts.
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
        FROM {database}.cancelAllUnfreezeV2Contracts 
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
            column_formats=TRON_CANCEL_ALL_UNFREEZE_V2_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def clear_abi_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get clearAbiContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of clearAbiContracts.
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
        FROM {database}.clearAbiContracts 
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
            column_formats=TRON_CLEAR_ABI_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def create_smart_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get createSmartContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of createSmartContracts.
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
        FROM {database}.createSmartContracts 
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
            column_formats=TRON_CREATE_SMART_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def delegate_resource_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get delegateResourceContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of delegateResourceContracts.
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
        FROM {database}.delegateResourceContracts 
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
            column_formats=TRON_DELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def exchange_create_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get exchangeCreateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of exchangeCreateContracts.
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
        FROM {database}.exchangeCreateContracts 
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
            column_formats=TRON_EXCHANGE_CREATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def exchange_inject_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get exchangeInjectContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of exchangeInjectContracts.
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
        FROM {database}.exchangeInjectContracts 
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
            column_formats=TRON_EXCHANGE_INJECT_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def exchange_transaction_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get exchangeTransactionContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of exchangeTransactionContracts.
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
        FROM {database}.exchangeTransactionContracts 
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
            column_formats=TRON_EXCHANGE_TRANSACTION_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def exchange_withdraw_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get exchangeWithdrawContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of exchangeWithdrawContracts.
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
        FROM {database}.exchangeWithdrawContracts 
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
            column_formats=TRON_EXCHANGE_WITHDRAW_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def freeze_balance_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get freezeBalanceContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of freezeBalanceContracts.
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
        FROM {database}.freezeBalanceContracts 
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
            column_formats=TRON_FREEZE_BALANCE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def freeze_balance_v2_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get freezeBalanceV2Contracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of freezeBalanceV2Contracts.
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
        FROM {database}.freezeBalanceV2Contracts 
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
            column_formats=TRON_FREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def market_cancel_order_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get marketCancelOrderContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of marketCancelOrderContracts.
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
        FROM {database}.marketCancelOrderContracts 
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
            column_formats=TRON_MARKET_CANCEL_ORDER_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def market_sell_asset_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get marketSellAssetContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of marketSellAssetContracts.
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
        FROM {database}.marketSellAssetContracts 
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
            column_formats=TRON_MARKET_SELL_ASSET_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def participate_asset_issue_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get participateAssetIssueContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of participateAssetIssueContracts.
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
        FROM {database}.participateAssetIssueContracts 
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
            column_formats=TRON_PARTICIPATE_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def proposal_approve_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get proposalApproveContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of proposalApproveContracts.
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
        FROM {database}.proposalApproveContracts 
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
            column_formats=TRON_PROPOSAL_APPROVE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def proposal_create_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get proposalCreateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of proposalCreateContracts.
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
        FROM {database}.proposalCreateContracts 
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
            column_formats=TRON_PROPOSAL_CREATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def proposal_delete_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get proposalDeleteContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of proposalDeleteContracts.
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
        FROM {database}.proposalDeleteContracts 
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
            column_formats=TRON_PROPOSAL_DELETE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def set_account_id_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get setAccountIdContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of setAccountIdContracts.
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
        FROM {database}.setAccountIdContracts 
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
            column_formats=TRON_SET_ACCOUNT_ID_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def shielded_transfer_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get shieldedTransferContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of shieldedTransferContracts.
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
        FROM {database}.shieldedTransferContracts 
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
            column_formats=TRON_SHIELDED_TRANSFER_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def transfer_asset_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get transferAssetContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of transferAssetContracts.
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
        FROM {database}.transferAssetContracts 
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
            column_formats=TRON_TRANSFER_ASSET_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def transfer_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get transferContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of transferContracts.
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
        FROM {database}.transferContracts 
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
            column_formats=TRON_TRANSFER_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def trigger_smart_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get triggerSmartContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of triggerSmartContracts.
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
        FROM {database}.triggerSmartContracts 
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
            column_formats=TRON_TRIGGER_SMART_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def undelegate_resource_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get undelegateResourceContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of undelegateResourceContracts.
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
        FROM {database}.undelegateResourceContracts 
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
            column_formats=TRON_UNDELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def unfreeze_asset_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get unfreezeAssetContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of unfreezeAssetContracts.
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
        FROM {database}.unfreezeAssetContracts 
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
            column_formats=TRON_UNFREEZE_ASSET_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def unfreeze_balance_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get unfreezeBalanceContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of unfreezeBalanceContracts.
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
        FROM {database}.unfreezeBalanceContracts 
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
            column_formats=TRON_UNFREEZE_BALANCE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def unfreeze_balance_v2_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get unfreezeBalanceV2Contracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of unfreezeBalanceV2Contracts.
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
        FROM {database}.unfreezeBalanceV2Contracts 
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
            column_formats=TRON_UNFREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def update_asset_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get updateAssetContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of updateAssetContracts.
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
        FROM {database}.updateAssetContracts 
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
            column_formats=TRON_UPDATE_ASSET_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def update_brokerage_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get updateBrokerageContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of updateBrokerageContracts.
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
        FROM {database}.updateBrokerageContracts 
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
            column_formats=TRON_UPDATE_BROKERAGE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def update_energy_limit_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get updateEnergyLimitContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of updateEnergyLimitContracts.
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
        FROM {database}.updateEnergyLimitContracts 
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
            column_formats=TRON_UPDATE_ENERGY_LIMIT_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def update_setting_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get updateSettingContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of updateSettingContract.
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
        FROM {database}.updateSettingContracts
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
            column_formats=TRON_UPDATE_SETTING_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def vote_asset_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get voteAssetContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of voteAssetContracts.
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
        FROM {database}.voteAssetContracts 
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
            column_formats=TRON_VOTE_ASSET_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def vote_witness_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get voteWitnessContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of voteWitnessContracts.
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
        FROM {database}.voteWitnessContracts
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
            column_formats=TRON_VOTE_WITNESS_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def withdraw_balance_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get withdrawBalanceContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of withdrawBalanceContracts.
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
        FROM {database}.withdrawBalanceContracts
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
            column_formats=TRON_WITHDRAW_BALANCE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def withdraw_expire_unfreeze_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get withdrawExpireUnfreezeContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of withdrawExpireUnfreezeContracts.
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
        FROM {database}.withdrawExpireUnfreezeContracts
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
            column_formats=TRON_WITHDRAW_EXPIRE_UNFREEZE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def witness_create_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get witnessCreateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of witnessCreateContracts.
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
        FROM {database}.witnessCreateContracts
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
            column_formats=TRON_WITNESS_CREATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)

    def witness_update_contracts(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = {"blockNum": True, "transactionHash": True, "logIndex": True},
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """Get witnessUpdateContracts from the database.

        Args:
            where (str): The WHERE clause.
            order_by (Dict[str, bool], optional): The ORDER BY clause. Defaults to blockNum, transactionHash, and logIndex ascending.
            limit (int, optional): The LIMIT clause. Defaults to 100.
            offset (int, optional): The OFFSET clause. Defaults to 0.
            parameters (Dict[str, Any], optional): The query parameters. Defaults to None.
        Returns:
            Generator[Dict[str, Any], None, None]: A generator of witnessUpdateContracts.
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
        FROM {database}.witnessUpdateContracts
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
            column_formats=TRON_WITNESS_UPDATE_CONTRACT_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [
                dict(zip(rows_stream.source.column_names, row)) for row in rows_stream
            ]
            return convert_bytes_to_hex_generator(ChainStyle.TRON, named_results)
