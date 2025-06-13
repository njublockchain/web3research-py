QUERY_FORMATS = {
    "FixedString*": "string",
}

ETHEREUM_BLOCK_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "hash": "string",  # FixedString(66) - 0x prefixed hex string
    "number": "int",
    "parentHash": "string",  # FixedString(66) - 0x prefixed hex string
    "uncles": "list[str]",  # Array(FixedString(66)) - list of 0x prefixed hex strings
    "totalDifficulty": "int",
    "miner": "string",  # String - 0x prefixed address
    "difficulty": "int",
    "nonce": "string",  # String - 0x prefixed hex
    "baseFeePerGas": "int",
    "gasLimit": "int",
    "gasUsed": "int",
    "extraData": "string",  # String - 0x prefixed hex
    "timestamp": "int",
    "size": "int",
}

ETHEREUM_TRANSACTION_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "hash": "string",  # FixedString(66) - 0x prefixed hex string
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "chainId": "int",
    "type": "int",
    "from": "string",  # String - 0x prefixed address
    "to": "string",  # Nullable(String) - 0x prefixed address
    "value": "int",
    "nonce": "int",
    "input": "string",  # String CODEC(ZSTD(6)) - 0x prefixed hex
    "gas": "int",
    "gasPrice": "int",
    "maxFeePerGas": "int",
    "maxPriorityFeePerGas": "int",
    "contractAddress": "string",  # Nullable(String) - 0x prefixed address
    "cumulativeGasUsed": "int",
    "effectiveGasPrice": "int",
    "gasUsed": "int",
    "status": "int",
}

ETHEREUM_TRACE_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockPosition": "int",
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionHash": "string",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "traceAddress": "list[int]",  # Array(UInt64)
    "subtraces": "int",
    "transactionPosition": "int",
    "error": "string",  # Nullable(String) CODEC(ZSTD(6))
    "actionType": "string",  # LowCardinality(String)
    "actionCallFrom": "string",  # Nullable(String) - 0x prefixed address
    "actionCallTo": "string",  # Nullable(String) - 0x prefixed address
    "actionCallValue": "int",
    "actionCallInput": "string",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "actionCallGas": "int",
    "actionCallType": "string",  # LowCardinality(String)
    "actionCreateFrom": "string",  # Nullable(String) - 0x prefixed address
    "actionCreateValue": "int",
    "actionCreateInit": "string",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "actionCreateGas": "int",
    "actionSuicideAddress": "string",  # Nullable(String) - 0x prefixed address
    "actionSuicideRefundAddress": "string",  # Nullable(String) - 0x prefixed address
    "actionSuicideBalance": "int",
    "actionRewardAuthor": "string",  # Nullable(String) - 0x prefixed address
    "actionRewardValue": "int",
    "actionRewardType": "string",  # LowCardinality(String)
    "resultType": "string",  # LowCardinality(String)
    "resultCallGasUsed": "int",
    "resultCallOutput": "string",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "resultCreateGasUsed": "int",
    "resultCreateCode": "string",  # Nullable(String) CODEC(ZSTD(6)) - 0x prefixed hex
    "resultCreateAddress": "string",  # Nullable(String) - 0x prefixed address
}

ETHEREUM_EVENT_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionHash": "string",  # FixedString(66) - 0x prefixed hex string
    "transactionIndex": "int",
    "logIndex": "int",
    "removed": "bool",
    "address": "string",  # String - 0x prefixed address
    "topic0": "string",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "topic1": "string",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "topic2": "string",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "topic3": "string",  # Nullable(FixedString(66)) - 0x prefixed hex string
    "data": "string",  # String CODEC(ZSTD(6)) - 0x prefixed hex string
}

ETHEREUM_ACCESS_LIST_ITEM_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(66) - 0x prefixed hex string
    "itemIndex": "int",
    "address": "string",  # String - 0x prefixed address
    "storageKey": "list[str]",  # Array(FixedString(66)) - list of 0x prefixed hex strings
}

ETHEREUM_WITHDRAWAL_COLUMN_FORMATS: dict[str, str | dict[str, str]] | None = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "index": "int",
    "validatorIndex": "int",
    "address": "string",  # String - 0x prefixed address
    "amount": "int",
}
