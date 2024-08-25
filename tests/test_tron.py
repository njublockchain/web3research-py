import os
import json
import unittest
import web3research
import web3
from web3research.evm import SingleEventDecoder, ContractDecoder
from web3research.common.types import Address


class TestTron(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        print("API Token: \t", os.environ["W3R_API_TOKEN"])
        print("Backend: \t", os.environ["W3R_BACKEND"])
        self._w3r = web3research.Web3Research(api_token=os.environ["W3R_API_TOKEN"])
        self._w3r_tron = self._w3r.tron(backend=os.environ["W3R_BACKEND"])

    def test_blocks(self):
        print(json.dumps(list(self._w3r_tron.blocks("number > 10000000", limit=5))))

    def test_transactions(self):
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        print(USDT_TAddr.addr_hex)
        print(json.dumps(list(self._w3r_tron.transactions(f"contractAddress={USDT_TAddr}", limit=5))))

    def test_events(self):
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        print(USDT_TAddr.addr_hex)
        print(json.dumps(list(self._w3r_tron.events(f"address={USDT_TAddr}", limit=5))))
    
    def test_single_event_decoder(self):
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        log = list(
            self._w3r_tron.events(
                f"address = {USDT_TAddr} and topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')",
                limit=1,
            )
        )[0]
        w3 = web3.Web3()
        abi = {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "from", "type": "address"},
                {"indexed": True, "name": "to", "type": "address"},
                {"indexed": False, "name": "value", "type": "uint256"},
            ],
            "name": "Transfer",
            "type": "event",
        }
        decoder = SingleEventDecoder(w3, event_abi=abi)
        print("log", log)
        result = decoder.decode(log)
        print(result)

    def test_transfer_contracts(self):
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        print(USDT_TAddr.addr_hex)
        self._w3r_tron.transfer_contracts(f"toAddress={USDT_TAddr}", limit=5)
        
if __name__ == "__main__":
    unittest.main()