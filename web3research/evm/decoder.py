from binascii import hexlify
from typing import Any, Dict, Sequence, Union

import eth_utils
from web3 import Web3

from web3research.common.types import Hash


class SingleEventDecoder:
    """SingleEventDecoder decodes a single event from a log object."""

    def __init__(self, web3: Web3, event_abi: Dict[str, Any], name=None):
        """Create a SingleEventDecoder instance.

        Args:
            web3 (Web3): A web3.Web3 instance.
            event_abi (Dict[str, Any]): The event ABI.
            name (str, optional): The event name. Defaults to None.
        """
        self.event_name = name or event_abi["name"]
        self.abi = [event_abi]
        self.contract = web3.eth.contract(abi=self.abi)

    def decode(self, event_log: Dict[str, Any]) -> Dict[str, Any]:
        """Decode an event log.

        Args:
            event_log (EventLog): The event log.
        Returns:
            Dict[str, Any]: The decoded event.
        """
        event = getattr(self.contract.events, self.event_name)
        return event().process_log(event_log)["args"]


class ContractDecoder:
    """ContractDecoder decodes events and function inputs from a contract ABI."""

    def __init__(self, web3: Web3, contract_abi: Sequence[Dict[str, Any]]):
        """Create a ContractDecoder instance.

        Args:
            web3 (Web3): A web3.Web3 instance.
            contract_abi (Sequence[Dict[str, Any]]): The contract ABI.
        Returns:
            ContractDecoder: A ContractDecoder instance.
        """
        self.abi = contract_abi
        self.contract = web3.eth.contract(abi=self.abi)

    def decode_event_log(
        self, event_name: str, event_log: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode an event log.

        Args:
            event_name (str): The event name.
            event_log (Dict[str, Any]): The event log.
        Returns:
            Dict[str, Any]: The decoded event.
        """
        event = getattr(self.contract.events, event_name)
        return event().process_log(event_log)["args"]

    def decode_function_input(self, input_data: Union[str, bytes]) -> Dict[str, Any]:
        """Decode a function input.

        Args:
            input_data (Union[str, bytes]): The input data.
        Returns:
            Dict[str, Any]: The decoded input.
        """
        return self.contract.decode_function_input(input_data)

    def get_event_abi(self, event_name: str):
        """Get the ABI of an event.

        Args:
            event_name (str): The event name.
        Returns:
            Dict[str, Any]: The event ABI.
        """
        for abi in self.abi:
            if abi["type"] == "event" and abi["name"] == event_name:
                return abi
        raise ValueError(
            "Event {event_name} not found in contract ABI".format(event_name=event_name)
        )

    def get_function_abi(self, function_name: str):
        """Get the ABI of a function.

        Args:
            function_name (str): The function name.
        Returns:
            Dict[str, Any]: The function ABI.
        """
        for abi in self.abi:
            if abi["type"] == "function" and abi["name"] == function_name:
                return abi
        raise ValueError(
            "Function {function_name} not found in contract ABI".format(
                function_name=function_name
            )
        )

    def get_event_topic(self, event_name: str):
        """Get the topic of an event.

        Args:
            event_name (str): The event name.
        Returns:
            str: The event topic.
        """
        event_abi = self.get_event_abi(event_name)

        return "0x" + hexlify(eth_utils.event_abi_to_log_topic(event_abi)).decode()

    def get_function_selector(self, function_name: str):
        """Get the selector of a function.

        Args:
            function_name (str): The function name.
        Returns:
            str: The function selector.
        """
        function_abi = self.get_function_abi(function_name)

        return (
            "0x"
            + hexlify(eth_utils.function_abi_to_4byte_selector(function_abi)).decode()
        )
