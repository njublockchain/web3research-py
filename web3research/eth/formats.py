ETHEREUM_BLOCK_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "hash": "str",  # FixedString(66) - 0x prefixed hex string
    "number": "int",
    "parentHash": "str",  # FixedString(66) - 0x prefixed hex string
    "uncles": "list[str]",  # Array(FixedString(66)) - list of 0x prefixed hex strings
    "totalDifficulty": "int",
    "miner": "str",  # String - 0x prefixed address
    "difficulty": "int",
    "nonce": "str",  # String - 0x prefixed hex
    "baseFeePerGas": "int",
    "gasLimit": "int",
    "gasUsed": "int",
    "extraData": "str",  # String - 0x prefixed hex
    "timestamp": "int",
    "size": "int",
}

ETHEREUM_TRANSACTION_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "hash": "str",  # FixedString(66) - 0x prefixed hex string
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "chainId": "int",
    "type": "int",
    "from": "str",  # String - 0x prefixed address
    "to": "str",  # Nullable(String) - 0x prefixed address
    "value": "int",
    "nonce": "int",
    "input": "str",  # String CODEC(ZSTD(6)) - 0x prefixed hex
    "gas": "int",
    "gasPrice": "int",
    "maxFeePerGas": "int",
    "maxPriorityFeePerGas": "int",
    "contractAddress": "str",  # Nullable(String) - 0x prefixed address
    "cumulativeGasUsed": "int",
    "effectiveGasPrice": "int",
    "gasUsed": "int",
    "status": "int",
}

ETHEREUM_TRACE_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockPosition": "int",
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionHash": "str",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "traceAddress": "list[int]",  # Array(UInt64)
    "subtraces": "int",
    "transactionPosition": "int",
    "error": "str",  # Nullable(String) CODEC(ZSTD(6))
    "actionType": "str",  # LowCardinality(String)
    "actionCallFrom": "str",  # Nullable(String) - 0x prefixed address
    "actionCallTo": "str",  # Nullable(String) - 0x prefixed address
    "actionCallValue": "int",
    "actionCallInput": "str",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "actionCallGas": "int",
    "actionCallType": "str",  # LowCardinality(String)
    "actionCreateFrom": "str",  # Nullable(String) - 0x prefixed address
    "actionCreateValue": "int",
    "actionCreateInit": "str",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "actionCreateGas": "int",
    "actionSuicideAddress": "str",  # Nullable(String) - 0x prefixed address
    "actionSuicideRefundAddress": "str",  # Nullable(String) - 0x prefixed address
    "actionSuicideBalance": "int",
    "actionRewardAuthor": "str",  # Nullable(String) - 0x prefixed address
    "actionRewardValue": "int",
    "actionRewardType": "str",  # LowCardinality(String)
    "resultType": "str",  # LowCardinality(String)
    "resultCallGasUsed": "int",
    "resultCallOutput": "str",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "resultCreateGasUsed": "int",
    "resultCreateCode": "str",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "resultCreateAddress": "str",  # Nullable(String) - 0x prefixed address
}

ETHEREUM_EVENT_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionHash": "str",  # FixedString(66) - 0x prefixed hex string
    "transactionIndex": "int",
    "logIndex": "int",
    "removed": "bool",
    "address": "str",  # String - 0x prefixed address
    "topic0": "str",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "topic1": "str",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "topic2": "str",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "topic3": "str",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "data": "str",  # String CODEC(ZSTD(6)) - 0x prefixed hex string
}

ETHEREUM_ACCESS_LIST_ITEM_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(66) - 0x prefixed hex string
    "itemIndex": "int",
    "address": "str",  # String - 0x prefixed address
    "storageKey": "list[str]",  # Array(FixedString(66)) - list of 0x prefixed hex strings
}

ETHEREUM_WITHDRAWAL_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "index": "int",
    "validatorIndex": "int",
    "address": "str",  # String - 0x prefixed address
    "amount": "int",
}
