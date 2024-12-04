from enum import Enum
from typing import Optional
import base58


class ChainStyle(str, Enum):
    BTC = "chain:btc"
    ETH = "chain:eth"
    TRON = "chain:tron"

class BTCAddressVariant(str, Enum):
    P2PKH = "p2pkh"
    P2SH = "p2sh"
    BECH32 = "bech32"


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
            if addr.startswith("0x") and len(addr) == 42:
                self.addr_hex = addr.removeprefix("0x")
                assert len(self.addr_hex) == 40, "Invalid ETH address"
            elif addr.startswith("T") and len(addr) == 34:
                self.addr_hex = base58.b58decode_check(addr)[1:].hex()
                assert len(self.addr_hex) == 40, "Invalid TRON address"
            elif addr.startswith("41") and len(addr) == 42:
                self.addr_hex = addr.removeprefix("41")
                assert len(self.addr_hex) == 40, "Invalid TRON address"
            elif len(addr) == 40:
                self.addr_hex = addr
            else:
                raise ValueError("Invalid address")
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

    def string(self, chain_style: ChainStyle, variant: Optional[BTCAddressVariant] = None):
        """Return the address string.

        Args:
            chain_style (ChainStyle): The chain style.
        Returns:
            str: The address string.
        """
        if chain_style == ChainStyle.BTC:
            if variant == BTCAddressVariant.P2PKH:
                return base58.b58encode_check(bytes.fromhex("00" + self.addr_hex)).decode()
            elif variant == BTCAddressVariant.P2SH:
                return base58.b58encode_check(bytes.fromhex("05" + self.addr_hex)).decode()
            elif variant == BTCAddressVariant.BECH32:
                raise ValueError("Bech32 not supported")
            elif variant is None:
                raise ValueError("BTC address variant required")
            else:
                raise ValueError("Invalid BTC address variant")
        elif chain_style == ChainStyle.TRON:
            return base58.b58encode_check(bytes.fromhex("41" + self.addr_hex)).decode()
        elif chain_style == ChainStyle.ETH:
            return "0x" + self.addr_hex
        else:
            raise ValueError("Invalid chain style")

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
            if hash.startswith("0x") and len(hash) == 66:
                self.hash_hex = hash.removeprefix("0x")
                assert len(self.hash_hex) == 64, "Invalid ETH hash"
            elif len(hash) == 64:
                self.hash_hex = hash
            else:
                raise ValueError("Invalid hash")
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

    def string(self, chain_style: ChainStyle):
        """Return the hash string.

        Args:
            chain_style (ChainStyle): The chain style.
        Returns:
            str: The hash string.
        """
        if chain_style == ChainStyle.BTC:
            return self.hash_hex
        elif chain_style == ChainStyle.TRON:
            return self.hash_hex
        elif chain_style == ChainStyle.ETH:
            return "0x" + self.hash_hex
        else:
            raise ValueError("Invalid chain style")
        