import unittest
from web3research.common.types import Address, ChainStyle


class TestCommon(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def test_address(self):
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        USDT_41Addr = "41"+USDT_TAddr.addr_hex
        print(USDT_41Addr)
        print(USDT_TAddr.string(ChainStyle.TRON))
        print(Address(USDT_TAddr.string(ChainStyle.TRON)).string(ChainStyle.TRON))
        print(Address(USDT_41Addr).string(ChainStyle.TRON))
    
        USDT_ETHAddr = Address("0xdac17f958d2ee523a2206206994597c13d831ec7")
        print(USDT_ETHAddr.addr_hex)
        print(USDT_ETHAddr.string(ChainStyle.ETH))
if __name__ == "__main__":
    unittest.main()
