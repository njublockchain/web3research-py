from kylink.eth.defi import DeFiProvider
from kylink.eth.market import MarketProvider
from kylink.db import ClickhouseProvider
from kylink.eth.resolve import ResolveProvider
from kylink.eth.token import TokenProvider
from kylink.eth.wallet import WalletProvider

_ETHEREUM_BLOCK_COLUMN_FORMATS = {
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

_ETHEREUM_TRANSACTION_COLUMN_FORMATS = {
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

_ETHEREUM_TRACE_COLUMN_FORMATS = {
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

_ETHEREUM_EVENT_COLUMN_FORMATS = {
    "address": "bytes",
    "blockHash": "bytes",
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionHash": "bytes",
    "transactionIndex": "int",
    "logIndex": "int",
    # "removed": "bool",
    # "topic0": "bytes",
    # "topic1": "bytes",
    # "topic2": "bytes",
    # "topic3": "bytes",
    "data": "bytes",
}


class EthereumProvider(ClickhouseProvider):
    def __init__(
        self,
        api_token,  # required
        host="db.kylink.xyz",
        port=443,
        database="ethereum",
        interface=None,
        settings=None,
        generic_args=None,
        **kwargs,
    ):
        super().__init__(
            host=host,
            port=port,
            api_token=api_token,
            database=database,
            interface=interface,
            settings=settings,
            generic_args=generic_args,
            **kwargs,
        )
        self.market = MarketProvider(self)
        self.defi = DeFiProvider(self)
        self.resolve = ResolveProvider(self)
        self.wallet = WalletProvider(self)
        self.token = TokenProvider(self)

    def blocks(self, where, limit=100, offset=0):
        limitSection = f"LIMIT {limit}" if limit > 0 else ""
        offsetSection = f"OFFSET {offset}" if offset > 0 else ""

        q = f"SELECT * FROM blocks WHERE {where} {limitSection} {offsetSection}"
        stream = self.query_rows_stream(
            q,
            column_formats=_ETHEREUM_BLOCK_COLUMN_FORMATS,  # avoid auto convert string to bytes
        )
        # convert QueryResult to list of json object
        with stream:
            column_names = stream.source.column_names

            blocks = [
                {col: block[i] for i, col in enumerate(column_names)}
                for block in stream
            ]

            return blocks

    def transactions(self, where, limit=100, offset=0):
        limitSection = f"LIMIT {limit}" if limit > 0 else ""
        offsetSection = f"OFFSET {offset}" if offset > 0 else ""

        q = f"SELECT * FROM transactions WHERE {where} {limitSection} {offsetSection}"
        stream = self.query_rows_stream(
            q, column_formats=_ETHEREUM_TRANSACTION_COLUMN_FORMATS
        )
        # convert QueryResult to list of json object
        with stream:
            column_names = stream.source.column_names

            transactions = [
                {col: transaction[i] for i, col in enumerate(column_names)}
                for transaction in stream
            ]

            return transactions

    def traces(self, where, limit=100, offset=0):
        limitSection = f"LIMIT {limit}" if limit > 0 else ""
        offsetSection = f"OFFSET {offset}" if offset > 0 else ""

        q = f"SELECT * FROM traces WHERE {where} {limitSection} {offsetSection}"
        stream = self.query_rows_stream(
            q, column_formats=_ETHEREUM_TRACE_COLUMN_FORMATS
        )
        # convert QueryResult to list of json object
        with stream:
            column_names = stream.source.column_names

            traces = [
                {col: trace[i] for i, col in enumerate(column_names)}
                for trace in stream
            ]

            return traces

    def events(self, where, limit=100, offset=0):
        limitSection = f"LIMIT {limit}" if limit > 0 else ""
        offsetSection = f"OFFSET {offset}" if offset > 0 else ""

        q = f"SELECT * FROM events WHERE {where} {limitSection} {offsetSection}"
        stream = self.query_rows_stream(
            q, column_formats=_ETHEREUM_EVENT_COLUMN_FORMATS
        )
        # convert QueryResult to list of json object
        with stream:
            column_names = stream.source.column_names

            events = []
            for e in stream:
                event = {col: e[i] for i, col in enumerate(column_names)}
                event["topics"] = []

                # restruct the topics
                if event["topic0"] is not None:
                    event["topics"].append(event["topic0"])
                if event["topic1"] is not None:
                    event["topics"].append(event["topic1"])
                if event["topic2"] is not None:
                    event["topics"].append(event["topic2"])
                if event["topic3"] is not None:
                    event["topics"].append(event["topic3"])

                del event["topic0"], event["topic1"], event["topic2"], event["topic3"]
                events.append(event)

            return events
