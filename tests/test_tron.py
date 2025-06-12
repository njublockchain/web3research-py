import os
import web3
import json
import pytest
import web3research

from web3research.evm import SingleEventDecoder, ContractDecoder
from web3research.common.types import Address


@pytest.fixture(scope="class")
def tron_client():
    """Fixture to provide Web3Research Tron client for testing."""
    api_token = os.environ.get("W3R_API_TOKEN", "default")
    backend = os.environ.get("W3R_BACKEND", "http://localhost:8123")
    print("API Token: \t", api_token)
    print("Backend: \t", backend)
    w3r = web3research.Web3Research(api_token=api_token)
    return w3r.tron(backend=backend)


class TestTron:
    """Test suite for Tron blockchain operations using Web3Research."""

    def test_blocks(self, tron_client):
        """Test fetching Tron blocks."""
        blocks = list(tron_client.blocks("number > 10000000", limit=5))
        assert isinstance(blocks, list), "Blocks should be returned as a list"
        assert len(blocks) <= 5, "Should not return more than 5 blocks"
        
        # Print for debugging
        print(json.dumps(blocks))

    def test_transactions(self, tron_client):
        """Test fetching Tron transactions for USDT contract."""
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        assert USDT_TAddr.addr_hex, "USDT address hex should not be empty"
        
        transactions = list(tron_client.transactions(f"contractAddress={USDT_TAddr}", limit=5))
        assert isinstance(transactions, list), "Transactions should be returned as a list"
        assert len(transactions) <= 5, "Should not return more than 5 transactions"
        
        # Print for debugging
        print(USDT_TAddr.addr_hex)
        print(json.dumps(transactions))

    def test_events(self, tron_client):
        """Test fetching Tron events for USDT contract."""
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        assert USDT_TAddr.addr_hex, "USDT address hex should not be empty"
        
        events = list(tron_client.events(f"address={USDT_TAddr}", limit=5))
        assert isinstance(events, list), "Events should be returned as a list"
        assert len(events) <= 5, "Should not return more than 5 events"
        
        # Print for debugging
        print(USDT_TAddr.addr_hex)
        print(json.dumps(events))
    
    def test_single_event_decoder(self, tron_client):
        """Test single event decoding for USDT Transfer events on Tron."""
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        events = list(
            tron_client.events(
                f"address = {USDT_TAddr} and topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')",
                limit=1,
            )
        )
        
        if not events:
            pytest.skip("No USDT Transfer events found on Tron")
            
        log = events[0]
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
        
        assert result is not None, "Decoded result should not be None"
        assert "from" in result or "to" in result, "Result should contain transfer information"
        
        # Print for debugging
        print("log", log)
        print(result)

    def test_transfer_contracts(self, tron_client):
        """Test fetching transfer contracts for USDT address."""
        USDT_TAddr = Address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        assert USDT_TAddr.addr_hex, "USDT address hex should not be empty"
        
        try:
            transfers = list(tron_client.transfer_contracts(f"toAddress={USDT_TAddr}", limit=5))
            assert isinstance(transfers, list), "Transfer contracts should be returned as a list"
            assert len(transfers) <= 5, "Should not return more than 5 transfer contracts"
        except AttributeError:
            pytest.skip("transfer_contracts method not available")
        
        # Print for debugging
        print(USDT_TAddr.addr_hex)
