"""Pytest configuration for async tests."""

import pytest


# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)


@pytest.fixture(scope="session")
def event_loop_policy():
    """Set the event loop policy for async tests."""
    import asyncio
    
    # Use the default event loop policy
    return asyncio.DefaultEventLoopPolicy()