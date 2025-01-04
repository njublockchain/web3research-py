from .endpoints import w3

vitalik_EOA_address = "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B"


def test_w3():
    assert w3.is_connected() == True
    assert w3.eth.chain_id == 1
