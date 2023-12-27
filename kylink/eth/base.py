from kylink.eth.defi import DeFiProvider
from kylink.eth.market import MarketProvider
from kylink.db import ClickhouseProvider
from kylink.eth.resolve import ResolveProvider
from kylink.eth.token import TokenProvider
from kylink.eth.wallet import WalletProvider

BLOCK_FIELD_TYPES = {
    "hash": bytes,
    "number": int,
    "parentHash": bytes,
    "uncles": bytes,
    "sha3Uncles": bytes,
    "totalDifficulty": int,
    "miner": bytes,
    "difficulty": int,
    "nonce": int,
    "mixHash": bytes,
    "baseFeePerGas": int,
    "gasLimit": int,
    "gasUsed": int,
    "stateRoot": bytes,
    "transactionsRoot": bytes,
    "receiptsRoot": bytes,
    "logsBloom": bytes,
    "withdrawlsRoot": bytes,
    "extraData": bytes,
    "timestamp": int,
    "size": int,
}


def convert_type_by_field_name(field_name, value):
    if field_name in BLOCK_FIELD_TYPES:
        return BLOCK_FIELD_TYPES[field_name](value)
    else:
        return value


class EthereumProvider(ClickhouseProvider):
    def __init__(self, api_token=None):
        super().__init__(api_token=api_token, database="ethereum")
        self.market = MarketProvider(self)
        self.defi = DeFiProvider(self)
        self.resolve = ResolveProvider(self)
        self.wallet = WalletProvider(self)
        self.token = TokenProvider(self)

    def blocks(self, where, limit=100, offset=0):
        limitSection = f"LIMIT {limit}" if limit > 0 else ""
        offsetSection = f"OFFSET {offset}" if offset > 0 else ""

        q = f"SELECT * FROM blocks WHERE {where} {limitSection} {offsetSection}"
        stream = self.query_rows_stream(q)
        # convert QueryResult to list of json object
        with stream:
            column_names = stream.source.column_names
            print("column_names", column_names)

            blocks = [
                {
                    col: convert_type_by_field_name(col, block[i])
                    for i, col in enumerate(column_names)
                }
                for block in stream
            ]

            return blocks
