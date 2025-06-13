import base58
from binascii import unhexlify
from typing import Any, Dict, Generator, Optional


def eth_to_tron_address(eth_address: str) -> str:
    """Convert Ethereum address to TRON address.

    Args:
        eth_address (str): The Ethereum address.
    Returns:
        str: The TRON address.
    """
    if not eth_address.startswith("0x") and len(eth_address) != 40:
        raise ValueError(
            "Not a valid Ethereum address: {eth_address}".format(
                eth_address=eth_address
            )
        )

    # Remove the '0x' prefix and convert to bytes
    eth_address_bytes = unhexlify(eth_address.removeprefix("0x").lower())

    # Convert to base58check encoding
    tron_address = base58.b58encode_check(b"\x41" + eth_address_bytes).decode("utf-8")

    return tron_address


def group_event_topics(event: dict):
    """Group event topics into a list and restructure the event dictionary.

    Args:
        event (dict): The event dictionary.
    Returns:
        dict: The modified event dictionary.
    """
    event["topics"] = []
    # restruct the topics
    if event["topic0"] is not None:
        event["topics"].append(event["topic0"])
    if event["topic1"] is not None:
        event["topics"].append(event["topic1"])
    if event["topic2"] is not None:
        event["topics"].append(event["topic2"])
    if event["topic3"] is not None:
        event["topics"].append(event["topic3"])

    del event["topic0"], event["topic1"], event["topic2"], event["topic3"]

    # compatible to TRON
    if "transactionIndex" not in event:
        event["transactionIndex"] = 0
    if "blockHash" not in event:
        event["blockHash"] = None
    if "blockNumber" not in event:
        event["blockNumber"] = 0

    return event


def use_tron_address(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert hex address to TRON address in a dictionary.

    Args:
        item (Dict[str, Any]): The dictionary containing addresses.
    Returns:
        Dict[str, Any]: The dictionary with TRON addresses.
    """
    for key, value in item.items():
        if isinstance(value, str) and len(value) == 40:
            # Convert Ethereum address to TRON address
            item[key] = eth_to_tron_address(value)
        elif isinstance(value, list):
            # Convert each Ethereum address in the list to TRON address
            item[key] = [
                eth_to_tron_address(addr)
                for addr in value
                if isinstance(addr, str) and len(addr) == 40
            ]
        elif isinstance(value, dict):
            use_tron_address(value)  # Recursively convert nested dictionaries
        else:
            # If the value is not an address, leave it unchanged
            continue

    return item
