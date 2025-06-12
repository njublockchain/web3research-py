import pytest
from web3research.common.types import Address, ChainStyle


class TestCommon:
    """Test suite for common types and utilities."""
    
    def test_address(self):
        """Test Address type functionality for both TRON and Ethereum addresses."""
        # Test TRON address
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        USDT_41Addr = "41" + USDT_TAddr.addr_hex
        
        # Assertions for TRON address
        assert USDT_TAddr.addr_hex, "TRON address hex should not be empty"
        assert USDT_TAddr.string(ChainStyle.TRON) == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "TRON address string should match original"
        assert Address(USDT_TAddr.string(ChainStyle.TRON)).string(ChainStyle.TRON) == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "Address round-trip should work"
        assert Address(USDT_41Addr).string(ChainStyle.TRON) == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "41-prefixed address should convert correctly"
        
        # Test Ethereum address
        USDT_ETHAddr = Address("0xdac17f958d2ee523a2206206994597c13d831ec7")
        
        # Assertions for Ethereum address
        assert USDT_ETHAddr.addr_hex, "Ethereum address hex should not be empty"
        assert USDT_ETHAddr.string(ChainStyle.ETH) == "0xdac17f958d2ee523a2206206994597c13d831ec7", "Ethereum address string should match original"
        
        # Print for debugging
        print(USDT_41Addr)
        print(USDT_TAddr.string(ChainStyle.TRON))
        print(Address(USDT_TAddr.string(ChainStyle.TRON)).string(ChainStyle.TRON))
        print(Address(USDT_41Addr).string(ChainStyle.TRON))
        print(USDT_ETHAddr.addr_hex)
        print(USDT_ETHAddr.string(ChainStyle.ETH))
