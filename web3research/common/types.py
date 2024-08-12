from enum import StrEnum
import base58


class ChainStyle(StrEnum):
    BTC = "btc"
    ETH = "eth"
    TRON = "tron"


class Address:
    def __init__(self, addr: str = None, addr_hex: str = None):
        if addr:
            self.addr = addr
            if addr.startswith("0x"):
                self.addr_hex = addr.removeprefix("0x")
            if addr.startswith("T"):
                self.addr_hex = base58.b58decode(addr)[1:].hex()
        else:
            assert (addr_hex is not None, "Either addr or addr_hex must be provided")
            self.addr_hex = addr_hex

    def __repr__(self):
        return "unhex('{addr_hex}')".format(addr_hex=self.addr_hex)

    def __eq__(self, other):
        return isinstance(other, Address) and self.addr_hex == other.addr_hex

    def __hash__(self):
        return self.addr


class Hash:
    def __init__(self, hash: str):
        self.hash = hash
        if hash.startswith("0x"):
            self.hash_hex = hash.removeprefix("0x")

    def __repr__(self):
        return "unhex('{hash_hex}')".format(hash_hex=self.hash_hex)

    def __eq__(self, other):
        return isinstance(other, Hash) and self.hash_hex == other.hash_hex

    def __hash__(self):
        return self.hash
