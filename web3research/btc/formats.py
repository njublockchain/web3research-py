QUERY_FORMATS = {
    "FixedString*": "string",
}

BITCOIN_BLOCK_COLUMN_FORMATS: dict[str, str | dict[str, str]] = {
    "height": "int",  # UInt64
    "hash": "string",  # FixedString(64) - hex string without 0x prefix
    "totalSize": "int",  # UInt32
    "weight": "int",  # UInt64
    "prevBlockHash": "string",  # FixedString(64) - hex string without 0x prefix
    "version": "int",  # Int32 - Block version for soft fork signalling
    "merkleRoot": "string",  # FixedString(64) - hex string without 0x prefix
    "time": "int",  # UInt32 - Block timestamp
    "bits": "int",  # UInt32 - Target value for blockhash
    "nonce": "int",  # UInt32 - Nonce for mining
    "difficulty": "float",  # Float64 CODEC(Gorilla) - Mining difficulty
}

BITCOIN_INPUT_COLUMN_FORMATS: dict[str, str | dict[str, str]] = {
    "txid": "string",  # FixedString(64) - hex string without 0x prefix
    "txIndex": "int",  # UInt32 - Transaction index in block
    "totalSize": "int",  # UInt32 - Total transaction size
    "baseSize": "int",  # UInt32 - Base transaction size
    "vsize": "int",  # UInt32 - Virtual transaction size
    "weight": "int",  # UInt64 - Transaction weight
    "version": "int",  # Int32 - Transaction version
    "lockTime": "int",  # UInt32 - Transaction lock time
    "blockHash": "string",  # FixedString(64) - hex string without 0x prefix
    "blockHeight": "int",  # UInt64 - Block height
    "blockTime": "int",  # UInt32 - Block timestamp
    "index": "int",  # UInt32 - Input index in transaction
    "prevOutputTxid": "string",  # FixedString(64) - Previous output transaction ID
    "prevOutputVout": "int",  # UInt32 - Previous output index
    "scriptSig": "string",  # String CODEC(ZSTD(6)) - Script signature hex
    "address": "string",  # Nullable(String) CODEC(ZSTD(6)) - Bitcoin address
    "sequence": "int",  # UInt32 - Input sequence number
    "witness": "list[str]",  # Array(String) CODEC(ZSTD(6)) - Witness data hex strings
}

BITCOIN_OUTPUT_COLUMN_FORMATS: dict[str, str | dict[str, str]] = {
    "txid": "string",  # FixedString(64) - hex string without 0x prefix
    "txIndex": "int",  # UInt32 - Transaction index in block
    "totalSize": "int",  # UInt32 - Total transaction size
    "baseSize": "int",  # UInt32 - Base transaction size
    "vsize": "int",  # UInt32 - Virtual transaction size
    "weight": "int",  # UInt64 - Transaction weight
    "version": "int",  # Int32 - Transaction version
    "lockTime": "int",  # UInt32 - Transaction lock time
    "blockHash": "string",  # String - Block hash hex string
    "blockHeight": "int",  # UInt64 - Block height
    "blockTime": "int",  # UInt32 - Block timestamp
    "index": "int",  # UInt32 - Output index in transaction
    "value": "int",  # UInt64 - Output value in satoshis
    "scriptPubkey": "string",  # String CODEC(ZSTD(6)) - Script pubkey hex
    "address": "string",  # Nullable(String) CODEC(ZSTD(6)) - Bitcoin address
}
