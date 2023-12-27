import os
import unittest
import kylink
import web3
from kylink.evm import SingleEventDecoder


class TestEthereum(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self._kylink = kylink.Kylink(api_token=os.environ["KYLINK_API_TOKEN"])

    # def test_blocks(self):
    #     print(self._kylink.eth.blocks("number > 10000000", limit=1))

    def test_decode(self):
        log = self._kylink.eth.events(
            "address = unhex('dac17f958d2ee523a2206206994597c13d831ec7')", limit=1
        )[0]
        print(log)
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
        result = decoder.decode(log)
        print(result)

if __name__ == "__main__":
    unittest.main()
