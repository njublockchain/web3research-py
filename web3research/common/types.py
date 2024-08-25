from enum import StrEnum
from typing import Optional
import base58


class ChainStyle(StrEnum):
    BTC = "btc"
    ETH = "eth"
    TRON = "tron"


class Address:
    """Address is a class for representing an address."""
    def __init__(self, addr: Optional[str] = None, addr_hex: Optional[str] = None):
        """Create an Address instance.

        Args:
            addr (str, optional): The address. Defaults to None.
            addr_hex (str, optional): The address hex. Defaults to None.
        Returns:
            Address: An Address instance
        """
        if addr:
            self.addr = addr
            if addr.startswith("0x"):
                self.addr_hex = addr.removeprefix("0x")
            elif addr.startswith("T"):
                self.addr_hex = base58.b58decode_check(addr)[1:].hex()
                assert len(self.addr_hex) == 40, "Invalid TRON address"
            else:
                self.addr_hex = addr
        else:
            assert addr_hex is not None, "Either addr or addr_hex must be provided"
            self.addr = addr_hex
            self.addr_hex = addr_hex

    def __repr__(self):
        return "unhex('{addr_hex}')".format(addr_hex=self.addr_hex)

    def __eq__(self, other):
        return isinstance(other, Address) and self.addr_hex == other.addr_hex

    def __hash__(self):
        return self.addr


class Hash:
    """Hash is a class for representing a hash."""
    def __init__(self, hash: Optional[str], hash_hex: Optional[str] = None):
        """Create a Hash instance.

        Args:
            hash (str): The hash.
        Returns:
            Hash: A Hash instance
        """
        if hash:
            self.hash = hash
            if hash.startswith("0x"):
                self.hash_hex = hash.removeprefix("0x")
            else:
                self.hash_hex = hash
        else:
            assert hash_hex is not None, "Either hash or hash_hex must be provided"
            self.hash = hash_hex
            self.hash_hex = hash_hex

    def __repr__(self):
        return "unhex('{hash_hex}')".format(hash_hex=self.hash_hex)

    def __eq__(self, other):
        return isinstance(other, Hash) and self.hash_hex == other.hash_hex

    def __hash__(self):
        return self.hash
