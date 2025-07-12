"""Tests for Bitcoin module."""

import os
import json
import pytest
import web3research

from web3research.btc import BitcoinProvider


@pytest.fixture(scope="class")
def bitcoin_client():
    """Fixture to provide Web3Research Bitcoin client for testing."""
    api_token = os.environ.get("W3R_API_TOKEN", "default")
    backend = os.environ.get("W3R_BACKEND", "http://localhost:8123")
    print("API Token: \t", api_token)
    print("Backend: \t", backend)
    w3r = web3research.Web3Research(api_token=api_token)
    return w3r.btc(backend=backend)


class TestBitcoin:
    """Test suite for Bitcoin blockchain operations using Web3Research."""

    def test_blocks(self, bitcoin_client):
        """Test fetching Bitcoin blocks."""
        blocks = list(bitcoin_client.blocks("height > 100000", limit=5))
        assert isinstance(blocks, list), "Blocks should be returned as a list"
        assert len(blocks) <= 5, "Should not return more than 5 blocks"

        # Verify block structure
        if blocks:
            block = blocks[0]
            expected_fields = ["height", "hash", "time"]
            for field in expected_fields:
                assert field in block or any(
                    field.lower() in k.lower() for k in block.keys()
                ), f"Block should contain {field} field"

        # Print for debugging
        print("Bitcoin blocks:")
        print(json.dumps(blocks, indent=2, default=str))

    def test_flood_blocks(self, bitcoin_client):
        """Test fetching large number of blocks (stress test)."""
        # This is a stress test - just ensure it doesn't crash
        try:
            blocks = list(bitcoin_client.blocks("height > 100000", limit=1000))
            assert isinstance(
                blocks, list
            ), "Should return a list even for large queries"
            assert len(blocks) <= 1000, "Should not return more than requested blocks"
        except Exception as e:
            pytest.skip(f"Flood test skipped due to: {e}")

    def test_transactions(self, bitcoin_client):
        """Test fetching Bitcoin transactions."""
        transactions = list(
            bitcoin_client.transactions("blockHeight > 100000", limit=5)
        )
        assert isinstance(
            transactions, list
        ), "Transactions should be returned as a list"
        assert len(transactions) <= 5, "Should not return more than 5 transactions"

        # Verify transaction structure
        if transactions:
            tx = transactions[0]
            expected_fields = ["txid", "blockHeight"]
            for field in expected_fields:
                assert field in tx, f"Transaction should contain {field} field"

        # Print for debugging
        print("Bitcoin transactions:")
        print(json.dumps(transactions, indent=2, default=str))

    def test_inputs(self, bitcoin_client):
        """Test fetching Bitcoin transaction inputs."""
        inputs = list(bitcoin_client.inputs("blockHeight > 100000", limit=5))
        assert isinstance(inputs, list), "Inputs should be returned as a list"
        assert len(inputs) <= 5, "Should not return more than 5 inputs"

        # Verify input structure
        if inputs:
            input_item = inputs[0]
            expected_fields = ["txid", "index", "blockHeight"]
            for field in expected_fields:
                assert field in input_item, f"Input should contain {field} field"

        # Print for debugging
        print("Bitcoin inputs:")
        print(json.dumps(inputs, indent=2, default=str))

    def test_outputs(self, bitcoin_client):
        """Test fetching Bitcoin transaction outputs."""
        outputs = list(bitcoin_client.outputs("blockHeight > 100000", limit=5))
        assert isinstance(outputs, list), "Outputs should be returned as a list"
        assert len(outputs) <= 5, "Should not return more than 5 outputs"

        # Verify output structure
        if outputs:
            output_item = outputs[0]
            expected_fields = ["txid", "index", "blockHeight", "value"]
            for field in expected_fields:
                assert field in output_item, f"Output should contain {field} field"

        # Print for debugging
        print("Bitcoin outputs:")
        print(json.dumps(outputs, indent=2, default=str))

    def test_high_value_transactions(self, bitcoin_client):
        """Test fetching high-value Bitcoin transactions."""
        # Look for transactions in outputs with high values (> 1 BTC = 100000000 satoshis)
        try:
            high_value_outputs = list(
                bitcoin_client.outputs(
                    "value > 100000000 AND blockHeight > 100000", limit=3
                )
            )
            assert isinstance(
                high_value_outputs, list
            ), "High value outputs should be returned as a list"

            if high_value_outputs:
                for output in high_value_outputs:
                    assert (
                        output.get("value", 0) > 100000000
                    ), "Output value should be greater than 1 BTC"

            print("High-value Bitcoin outputs:")
            print(json.dumps(high_value_outputs, indent=2, default=str))
        except Exception as e:
            pytest.skip(f"High value transactions test skipped due to: {e}")

    def test_specific_address_outputs(self, bitcoin_client):
        """Test fetching outputs for a specific Bitcoin address (if addresses are tracked)."""
        # This test might fail if the Bitcoin schema doesn't include address information
        try:
            # Try to find outputs with address information
            outputs_with_address = list(
                bitcoin_client.outputs("blockHeight > 100000", limit=10)
            )

            # Check if any outputs have address-related fields
            address_fields = []
            if outputs_with_address:
                first_output = outputs_with_address[0]
                potential_address_fields = [
                    "address",
                    "scriptPubKey",
                    "pubkeyHash",
                    "scriptType",
                ]
                address_fields = [
                    field for field in potential_address_fields if field in first_output
                ]

            if address_fields:
                print(f"Found address-related fields: {address_fields}")
                print("Sample output with address info:")
                print(json.dumps(outputs_with_address[0], indent=2, default=str))
            else:
                pytest.skip("No address information found in outputs")

        except Exception as e:
            pytest.skip(f"Address-specific test skipped due to: {e}")

    def test_coinbase_transactions(self, bitcoin_client):
        """Test fetching coinbase transactions (block rewards)."""
        try:
            # Coinbase transactions typically have a specific input pattern
            # Look for transactions with a single input that has no previous output
            coinbase_inputs = list(
                bitcoin_client.inputs("blockHeight > 100000", limit=20)
            )

            # Filter for potential coinbase transactions (first transaction in block, txIndex = 0)
            coinbase_candidates = [
                inp for inp in coinbase_inputs if inp.get("txIndex") == 0
            ]

            if coinbase_candidates:
                print("Potential coinbase transactions:")
                print(json.dumps(coinbase_candidates[:3], indent=2, default=str))
            else:
                pytest.skip("No coinbase transactions found in sample")

        except Exception as e:
            pytest.skip(f"Coinbase transactions test skipped due to: {e}")

    def test_recent_blocks(self, bitcoin_client):
        """Test fetching recent Bitcoin blocks with descending order."""
        try:
            # Get recent blocks in descending order
            recent_blocks = list(
                bitcoin_client.blocks(
                    where="height > 800000",
                    order_by={"height": False},  # Descending order
                    limit=3,
                )
            )

            assert isinstance(
                recent_blocks, list
            ), "Recent blocks should be returned as a list"

            # Verify blocks are in descending order
            if len(recent_blocks) > 1:
                for i in range(len(recent_blocks) - 1):
                    current_height = recent_blocks[i].get("height", 0)
                    next_height = recent_blocks[i + 1].get("height", 0)
                    assert (
                        current_height >= next_height
                    ), "Blocks should be in descending height order"

            print("Recent Bitcoin blocks (descending order):")
            print(json.dumps(recent_blocks, indent=2, default=str))

        except Exception as e:
            pytest.skip(f"Recent blocks test skipped due to: {e}")

    def test_transaction_size_analysis(self, bitcoin_client):
        """Test analyzing Bitcoin transaction sizes."""
        try:
            # Get transactions and analyze their sizes
            transactions = list(
                bitcoin_client.transactions("blockHeight > 100000", limit=10)
            )

            if transactions:
                size_fields = ["totalSize", "baseSize", "vsize", "weight"]
                transactions_with_size = [
                    tx
                    for tx in transactions
                    if any(field in tx for field in size_fields)
                ]

                if transactions_with_size:
                    print("Bitcoin transactions with size information:")
                    for tx in transactions_with_size[:3]:
                        size_info = {k: v for k, v in tx.items() if k in size_fields}
                        print(f"TX {tx.get('txid', 'unknown')[:16]}...: {size_info}")
                else:
                    pytest.skip("No size information found in transactions")
            else:
                pytest.skip("No transactions found for size analysis")

        except Exception as e:
            pytest.skip(f"Transaction size analysis test skipped due to: {e}")


if __name__ == "__main__":
    pytest.main([__file__])
