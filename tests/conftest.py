"""
Pytest configuration and shared fixtures for web3research tests.
"""
import os
import pytest


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring external services"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running test"
    )


@pytest.fixture(scope="session")
def api_credentials():
    """Provide API credentials for testing."""
    return {
        "api_token": os.environ.get("W3R_API_TOKEN", "default"),
        "backend": os.environ.get("W3R_BACKEND", "http://localhost:8123")
    }


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their names."""
    for item in items:
        # Mark tests that likely require external services
        if any(keyword in item.name for keyword in ["flood", "events", "transactions", "blocks"]):
            item.add_marker(pytest.mark.integration)
        
        # Mark potentially slow tests
        if "flood" in item.name:
            item.add_marker(pytest.mark.slow)
