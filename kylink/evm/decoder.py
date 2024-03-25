from typing import Any, Dict, TypedDict

from web3 import Web3


class Log(TypedDict):
    address: bytes
    topics: list[bytes]
    data: bytes
    logIndex: int
    transactionIndex: int
    transactionHash: bytes
    blockHash: bytes
    blockNumber: int


class SingleEventDecoder:
    def __init__(self, w3: Web3, event_abi: Dict[str, Any], name=None):
        self.event_name = name or event_abi["name"]
        self.abi = [event_abi]
        self.contract = w3.eth.contract(abi=self.abi)

    def decode(self, log: Log) -> Dict[str, Any]:
        event = getattr(self.contract.events, self.event_name)
        return event().process_log(log)["args"]


class ContractDecoder:
    def __init__(self, w3: Web3, contract_abi: Dict[str, Any], name=None):
        self.abi = contract_abi
        self.contract = w3.eth.contract(abi=self.abi)

    def decode_event_log(self, event_name: str, log: Log) -> Dict[str, Any]:
        event = getattr(self.contract.events, event_name)
        return event().process_log(log)["args"]

    def decode_function_input(self, input_data: bytes) -> Dict[str, Any]:
        return self.contract.decode_function_input(input_data)

    def get_event_abi(self, event_name: str):
        for abi in self.abi:
            if abi["type"] == "event" and abi["name"] == event_name:
                return abi
        raise ValueError(f"Event {event_name} not found in contract ABI")
    
    def get_function_abi(self, function_name: str):
        for abi in self.abi:
            if abi["type"] == "function" and abi["name"] == function_name:
                return abi
        raise ValueError(f"Function {function_name} not found in contract ABI")
    