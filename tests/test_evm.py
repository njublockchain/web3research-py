import unittest

import web3

from web3research.common.types import ChainStyle
from web3research.evm.abi import ERC20_ABI
from web3research.evm.decoder import ContractDecoder


class TestContractDecoder(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.w3 = web3.Web3()
        self.ABI = ERC20_ABI
        self.decoder = ContractDecoder(self.w3, contract_abi=self.ABI)
    
    def test_get_event_topic(self):
        topic = self.decoder.get_event_topic("Transfer")
        if topic != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
            raise ValueError("Invalid topic")

    def test_get_function_selector(self):
        signature = self.decoder.get_function_selector("transfer")
        if signature != "0xa9059cbb":
            raise ValueError("Invalid signature")
        