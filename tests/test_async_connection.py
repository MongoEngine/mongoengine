"""Test async connection functionality."""

import pytest
import pymongo

from mongoengine import (
    Document,
    StringField,
    connect,
    connect_async,
    disconnect,
    disconnect_async,
    get_db,
    get_async_db,
    is_async_connection,
)
from mongoengine.connection import ConnectionFailure


class TestAsyncConnection:
    """Test async connection management."""

    def teardown_method(self, method):
        """Clean up after each test."""
        # Disconnect all connections
        from mongoengine.connection import _connections, _dbs, _connection_types
        _connections.clear()
        _dbs.clear()
        _connection_types.clear()

    @pytest.mark.asyncio
    async def test_connect_async_basic(self):
        """Test basic async connection."""
        # Connect asynchronously
        client = await connect_async(db="mongoenginetest_async", alias="async_test")
        
        # Verify connection
        assert client is not None
        assert isinstance(client, pymongo.AsyncMongoClient)
        assert is_async_connection("async_test")
        
        # Get async database
        db = get_async_db("async_test")
        assert db is not None
        assert db.name == "mongoenginetest_async"
        
        # Clean up
        await disconnect_async("async_test")

    @pytest.mark.asyncio
    async def test_connect_async_with_existing_sync(self):
        """Test async connection when sync connection exists."""
        # Create sync connection first
        connect(db="mongoenginetest", alias="test_alias")
        assert not is_async_connection("test_alias")
        
        # Try to create async connection with same alias
        with pytest.raises(ConnectionFailure) as exc_info:
            await connect_async(db="mongoenginetest_async", alias="test_alias")
        
        # The error could be about different connection settings or sync connection
        error_msg = str(exc_info.value)
        assert "different connection" in error_msg or "synchronous connection" in error_msg
        
        # Clean up
        disconnect("test_alias")

    def test_connect_sync_with_existing_async(self):
        """Test sync connection when async connection exists."""
        # This test must be synchronous to test the sync connect function
        # We'll use pytest's event loop to run the async setup
        import asyncio
        
        async def setup():
            await connect_async(db="mongoenginetest_async", alias="test_alias")
        
        # Run async setup
        asyncio.run(setup())
        assert is_async_connection("test_alias")
        
        # Try to create sync connection with same alias
        with pytest.raises(ConnectionFailure) as exc_info:
            connect(db="mongoenginetest", alias="test_alias")
        
        # The error could be about different connection settings or async connection
        error_msg = str(exc_info.value)
        assert "different connection" in error_msg or "async connection" in error_msg
        
        # Clean up
        async def cleanup():
            await disconnect_async("test_alias")
        
        asyncio.run(cleanup())

    @pytest.mark.asyncio
    async def test_get_async_db_with_sync_connection(self):
        """Test get_async_db with sync connection raises error."""
        # Create sync connection
        connect(db="mongoenginetest", alias="sync_test")
        
        # Try to get async db
        with pytest.raises(ConnectionFailure) as exc_info:
            get_async_db("sync_test")
        
        assert "not async" in str(exc_info.value)
        
        # Clean up
        disconnect("sync_test")

    @pytest.mark.asyncio
    async def test_disconnect_async(self):
        """Test async disconnection."""
        # Connect
        await connect_async(db="mongoenginetest_async", alias="async_disconnect")
        assert is_async_connection("async_disconnect")
        
        # Disconnect
        await disconnect_async("async_disconnect")
        
        # Verify disconnection
        from mongoengine.connection import _connections, _connection_types
        assert "async_disconnect" not in _connections
        assert "async_disconnect" not in _connection_types

    @pytest.mark.asyncio
    async def test_multiple_async_connections(self):
        """Test multiple async connections with different aliases."""
        # Create multiple connections
        client1 = await connect_async(db="test_db1", alias="async1")
        client2 = await connect_async(db="test_db2", alias="async2")
        
        # Verify both are async
        assert is_async_connection("async1")
        assert is_async_connection("async2")
        
        # Verify different databases
        db1 = get_async_db("async1")
        db2 = get_async_db("async2")
        assert db1.name == "test_db1"
        assert db2.name == "test_db2"
        
        # Clean up
        await disconnect_async("async1")
        await disconnect_async("async2")

    @pytest.mark.asyncio
    async def test_reconnect_async_same_settings(self):
        """Test reconnecting with same settings."""
        # Initial connection
        await connect_async(db="mongoenginetest_async", alias="reconnect_test")
        
        # Reconnect with same settings (should not raise error)
        client = await connect_async(db="mongoenginetest_async", alias="reconnect_test")
        assert client is not None
        
        # Clean up
        await disconnect_async("reconnect_test")

    @pytest.mark.asyncio
    async def test_reconnect_async_different_settings(self):
        """Test reconnecting with different settings raises error."""
        # Initial connection
        await connect_async(db="mongoenginetest_async", alias="reconnect_test2")
        
        # Try to reconnect with different settings
        with pytest.raises(ConnectionFailure) as exc_info:
            await connect_async(db="different_db", alias="reconnect_test2")
        
        assert "different connection" in str(exc_info.value)
        
        # Clean up
        await disconnect_async("reconnect_test2")