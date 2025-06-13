import os
from typing import Union
from web3 import AsyncWeb3, HTTPProvider, Web3
from web3.providers.persistent import WebSocketProvider

DEFAULT_ETH_WEB3_ENDPOINT = "https://eth.web3resear.ch/v1/mainnet"
ETH_WEB3_ENDPOINT = os.environ.get("W3R_WEB3_ENDPOINT", DEFAULT_ETH_WEB3_ENDPOINT)

def get_ethereum_web3_endpoint(endpoint: str = ETH_WEB3_ENDPOINT) -> Union[Web3, AsyncWeb3]:
    """
    Get Ethereum web3 endpoint, supports both HTTP and WS protocols.
    Other ethereum-based chains can also use this function.
    By default, it uses the endpoint from the environment variable W3R_WEB3_ENDPOINT.
    If the environment variable is not set, it uses our default endpoint, which is
    "https://eth.web3resear.ch/v1/mainnet".

    Args:
        endpoint (str): The endpoint URL.

    Returns:
        Web3: The web3 instance.
    """
    if endpoint.startswith("http"):
        return Web3(HTTPProvider(endpoint))
    elif endpoint.startswith("ws"):
        return AsyncWeb3(WebSocketProvider(endpoint))
    else:
        raise ValueError(
            "Invalid endpoint protocol: {endpoint}".format(endpoint=endpoint)
        )


def get_tron_web3_endpoint(endpoint: str = ETH_WEB3_ENDPOINT) -> Web3:
    raise NotImplementedError("TRON web3 endpoint not implemented")


def get_btc_web3_endpoint(endpoint: str = ETH_WEB3_ENDPOINT) -> Web3:
    """
    Get Bitcoin web3 endpoint. Currently Bitcoin doesn't have native web3 support,
    so this is a placeholder for future Bitcoin RPC implementations.
    
    Args:
        endpoint (str): The endpoint URL.
        
    Returns:
        Web3: The web3 instance (placeholder).
    """
    raise NotImplementedError("Bitcoin web3 endpoint not implemented - Bitcoin uses different RPC protocols")
