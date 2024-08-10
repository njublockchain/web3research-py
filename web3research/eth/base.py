from typing import Any, Dict, Generator, Optional, Sequence
from web3research.eth.defi import DeFiProvider
from web3research.eth.market import MarketProvider
from web3research.db import ClickhouseProvider
from web3research.eth.resolve import ResolveProvider
from web3research.eth.token import TokenProvider
from web3research.eth.wallet import WalletProvider
from web3research.common.type_convert import (
    convert_bytes_to_hex_generator,
    group_events_generator,
)

ETHEREUM_BLOCK_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "hash": "bytes",
    "number": "int",
    "parentHash": "bytes",
    "uncles": "bytes",
    "sha3Uncles": "bytes",
    "totalDifficulty": "int",
    "miner": "bytes",
    "difficulty": "int",
    "nonce": "bytes",
    "mixHash": "bytes",
    "baseFeePerGas": "int",
    "gasLimit": "int",
    "gasUsed": "int",
    "stateRoot": "bytes",
    "transactionsRoot": "bytes",
    "receiptsRoot": "bytes",
    "logsBloom": "bytes",
    "withdrawlsRoot": "bytes",
    "extraData": "bytes",
    "timestamp": "int",
    "size": "int",
}

ETHEREUM_TRANSACTION_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "hash": "bytes",
    "blockHash": "bytes",
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "chainId": "int",
    "type": "int",
    "from": "bytes",
    "to": "bytes",
    "value": "int",
    "nonce": "int",
    "input": "bytes",
    "gas": "int",
    "gasPrice": "int",
    "maxFeePerGas": "int",
    "maxPriorityFeePerGas": "int",
    "r": "int",
    "s": "int",
    "v": "int",
    "accessList": "bytes",
    "contractAddress": "bytes",
    "cumulativeGasUsed": "int",
    "effectiveGasPrice": "int",
    "gasUsed": "int",
    "logsBloom": "bytes",
    "root": "bytes",
    "status": "int",
}

ETHEREUM_TRACE_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockPos": "int",
    "blockNumber": "int",
    "blockTimestamp": "int",
    "blockHash": "bytes",
    "transactionHash": "bytes",
    # "traceAddress": "list[int]",
    "subtraces": "int",
    "transactionPosition": "int",
    "error": "bytes",
    "actionType": "bytes",
    "actionCallFrom": "bytes",
    "actionCallTo": "bytes",
    "actionCallValue": "int",
    "actionCallInput": "bytes",
    "actionCallGas": "int",
    "actionCallType": "bytes",
    "actionCreateFrom": "bytes",
    "actionCreateValue": "int",
    "actionCreateInit": "bytes",
    "actionCreateGas": "int",
    "actionSuicideAddress": "bytes",
    "actionSuicideRefundAddress": "bytes",
    "actionSuicideBalance": "int",
    "actionRewardAuthor": "bytes",
    "actionRewardValue": "int",
    "actionRewardType": "bytes",
    "resultType": "bytes",
    "resultCallGasUsed": "int",
    "resultCallOutput": "bytes",
    "resultCreateGasUsed": "int",
    "resultCreateCode": "bytes",
    "resultCreateAddress": "bytes",
}

ETHEREUM_EVENT_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "address": "bytes",
    "blockHash": "bytes",
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionHash": "bytes",
    "transactionIndex": "int",
    "logIndex": "int",
    "removed": "bool",
    "topic0": "bytes",
    "topic1": "bytes",
    "topic2": "bytes",
    "topic3": "bytes",
    "data": "bytes",
}


class EthereumProvider(ClickhouseProvider):
    def __init__(
        self,
        api_token,  # required
        backend: Optional[str] = None,
        database: str = "ethereum",
        settings: Optional[Dict[str, Any]] = None,
        generic_args: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(
            api_token=api_token,
            backend=backend,
            database=database,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )
        self.database = database
        self.market = MarketProvider(self)
        self.defi = DeFiProvider(self)
        self.resolve = ResolveProvider(self)
        self.wallet = WalletProvider(self)
        self.token = TokenProvider(self)

    def blocks(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        where_phrase = f"WHERE {where}" if where else ""
        order_by_phrase = (
            f"ORDER BY {', '.join([f'{k} {"ASC" if v else "DESC"}' for k, v in order_by.items()])}"
            if order_by
            else ""
        )
        limit_phrase = f"LIMIT {limit}" if limit else ""
        offset_phrase = f"OFFSET {offset}" if offset else ""
        q = f"""
        SELECT * 
        FROM {self.database}.blocks 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """
        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_BLOCK_COLUMN_FORMATS,  # avoid auto convert string to bytes
            parameters={**(parameters or {})},
        )

        with rows_stream:
            named_results = [dict(zip(rows_stream.source.column_names, row)) for row in rows_stream]
            return convert_bytes_to_hex_generator(named_results)

    def transactions(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        where_phrase = f"WHERE {where}" if where else ""
        order_by_phrase = (
            f"ORDER BY {', '.join([f'{k} {"ASC" if v else "DESC"}' for k, v in order_by.items()])}"
            if order_by
            else ""
        )
        limit_phrase = f"LIMIT {limit}" if limit else ""
        offset_phrase = f"OFFSET {offset}" if offset else ""
        q = f"""
        SELECT * 
        FROM {self.database}.transactions 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_TRANSACTION_COLUMN_FORMATS,
            parameters={
                **(parameters or {}),
            },
        )

        with rows_stream:
            named_results = [dict(zip(rows_stream.source.column_names, row)) for row in rows_stream]
            return convert_bytes_to_hex_generator(named_results)
        
    def traces(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        where_phrase = f"WHERE {where}" if where else ""
        order_by_phrase = (
            f"ORDER BY {', '.join([f'{k} {"ASC" if v else "DESC"}' for k, v in order_by.items()])}"
            if order_by
            else ""
        )
        limit_phrase = f"LIMIT {limit}" if limit else ""
        offset_phrase = f"OFFSET {offset}" if offset else ""
        q = f"""
        SELECT * 
        FROM {self.database}.traces 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_TRACE_COLUMN_FORMATS,
            parameters={
                **(parameters or {}),
            },
        )
        
        with rows_stream:
            named_results = [dict(zip(rows_stream.source.column_names, row)) for row in rows_stream]
            return convert_bytes_to_hex_generator(named_results)

    def events(
        self,
        where: Optional[str],
        order_by: Optional[Dict[str, bool]] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        where_phrase = f"WHERE {where}" if where else ""
        order_by_phrase = (
            f"ORDER BY {', '.join([f'{k} {"ASC" if v else "DESC"}' for k, v in order_by.items()])}"
            if order_by
            else ""
        )
        limit_phrase = f"LIMIT {limit}" if limit else ""
        offset_phrase = f"OFFSET {offset}" if offset else ""
        q = f"""
        SELECT * 
        FROM {self.database}.events 
        {where_phrase} 
        {order_by_phrase} 
        {limit_phrase}
        {offset_phrase}
        """

        rows_stream = self.query_rows_stream(
            q,
            column_formats=ETHEREUM_EVENT_COLUMN_FORMATS,
            parameters={
                **(parameters or {}),
            },
        )

        with rows_stream:
            named_results = [dict(zip(rows_stream.source.column_names, row)) for row in rows_stream]
            return group_events_generator(convert_bytes_to_hex_generator(named_results))
