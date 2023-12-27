import os
import unittest
import kylink


class TestEthereum(unittest.TestCase):
    # def __init__(self, methodName: str = "runTest") -> None:
    #     super().__init__(methodName)

    def test_blocks(self):
        self._kylink = kylink.Kylink(api_token=os.environ["KYLINK_API_TOKEN"])
        print(self._kylink.eth.blocks("number > 10000000", limit=1))


if __name__ == "__main__":
    unittest.main()
