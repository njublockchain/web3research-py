import pytest
import web3

from web3research.common.types import ChainStyle
from web3research.evm.abi import ERC20_ABI
from web3research.evm.decoder import ContractDecoder


@pytest.fixture(scope="class")
def contract_decoder():
    """Fixture to provide ContractDecoder for testing."""
    w3 = web3.Web3()
    return ContractDecoder(w3, contract_abi=ERC20_ABI)


class TestContractDecoder:
    """Test suite for EVM contract decoder functionality."""

    def test_get_event_topic(self, contract_decoder):
        """Test getting event topic hash for Transfer event."""
        topic = contract_decoder.get_event_topic("Transfer")
        expected_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        
        assert topic == expected_topic, f"Expected topic {expected_topic}, got {topic}"

    def test_get_function_selector(self, contract_decoder):
        """Test getting function selector for transfer function."""
        signature = contract_decoder.get_function_selector("transfer")
        expected_signature = "0xa9059cbb"
        
        assert signature == expected_signature, f"Expected signature {expected_signature}, got {signature}"
