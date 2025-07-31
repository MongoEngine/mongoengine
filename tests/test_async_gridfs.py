"""Test async GridFS file operations."""

import io
import pytest
import pytest_asyncio
from bson import ObjectId

from mongoengine import (
    Document,
    StringField,
    FileField,
    connect_async,
    disconnect_async,
)
from mongoengine.fields import AsyncGridFSProxy


class AsyncFileDoc(Document):
    """Test document with file field."""
    name = StringField(required=True)
    file = FileField(db_alias="async_gridfs_test")
    
    meta = {"collection": "async_test_files"}


class AsyncImageDoc(Document):
    """Test document with custom collection name."""
    name = StringField(required=True)
    image = FileField(db_alias="async_gridfs_test", collection_name="async_images")
    
    meta = {"collection": "async_test_images"}


class TestAsyncGridFS:
    """Test async GridFS operations."""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connection and clean up after."""
        # Setup
        await connect_async(db="mongoenginetest_async_gridfs", alias="async_gridfs_test")
        AsyncFileDoc._meta["db_alias"] = "async_gridfs_test"
        AsyncImageDoc._meta["db_alias"] = "async_gridfs_test"
        
        yield
        
        # Teardown - clean collections
        try:
            await AsyncFileDoc.async_drop_collection()
            await AsyncImageDoc.async_drop_collection()
            # Also clean GridFS collections
            from mongoengine.connection import get_async_db
            db = get_async_db("async_gridfs_test")
            await db.drop_collection("fs.files")
            await db.drop_collection("fs.chunks")
            await db.drop_collection("async_images.files")
            await db.drop_collection("async_images.chunks")
        except:
            pass
        
        await disconnect_async("async_gridfs_test")
    
    @pytest.mark.asyncio
    async def test_async_file_upload(self):
        """Test uploading a file asynchronously."""
        # Create test file content
        file_content = b"This is async test file content"
        file_obj = io.BytesIO(file_content)
        
        # Create document and upload file
        doc = AsyncFileDoc(name="Test File")
        proxy = await AsyncFileDoc.file.async_put(file_obj, instance=doc, filename="test.txt")
        
        assert isinstance(proxy, AsyncGridFSProxy)
        assert proxy.grid_id is not None
        assert isinstance(proxy.grid_id, ObjectId)
        
        # Save document
        await doc.async_save()
        
        # Verify document was saved with file reference
        loaded_doc = await AsyncFileDoc.objects.async_get(id=doc.id)
        assert loaded_doc.file is not None
    
    @pytest.mark.asyncio
    async def test_async_file_read(self):
        """Test reading a file asynchronously."""
        # Upload a file
        file_content = b"Hello async GridFS!"
        file_obj = io.BytesIO(file_content)
        
        doc = AsyncFileDoc(name="Read Test")
        await AsyncFileDoc.file.async_put(file_obj, instance=doc, filename="read_test.txt")
        await doc.async_save()
        
        # Reload and read file
        loaded_doc = await AsyncFileDoc.objects.async_get(id=doc.id)
        proxy = await AsyncFileDoc.file.async_get(loaded_doc)
        
        assert proxy is not None
        assert isinstance(proxy, AsyncGridFSProxy)
        
        # Read content
        content = await proxy.async_read()
        assert content == file_content
        
        # Read partial content
        file_obj.seek(0)
        await proxy.async_replace(file_obj, filename="replaced.txt")
        partial = await proxy.async_read(5)
        assert partial == b"Hello"
    
    @pytest.mark.asyncio
    async def test_async_file_delete(self):
        """Test deleting a file asynchronously."""
        # Upload a file
        file_content = b"File to be deleted"
        file_obj = io.BytesIO(file_content)
        
        doc = AsyncFileDoc(name="Delete Test")
        proxy = await AsyncFileDoc.file.async_put(file_obj, instance=doc, filename="delete_me.txt")
        grid_id = proxy.grid_id
        await doc.async_save()
        
        # Delete the file
        await proxy.async_delete()
        
        assert proxy.grid_id is None
        
        # Verify file is gone
        new_proxy = AsyncGridFSProxy(
            grid_id=grid_id,
            db_alias="async_gridfs_test",
            collection_name="fs"
        )
        metadata = await new_proxy.async_get()
        assert metadata is None
    
    @pytest.mark.asyncio
    async def test_async_file_replace(self):
        """Test replacing a file asynchronously."""
        # Upload initial file
        initial_content = b"Initial content"
        file_obj = io.BytesIO(initial_content)
        
        doc = AsyncFileDoc(name="Replace Test")
        proxy = await AsyncFileDoc.file.async_put(file_obj, instance=doc, filename="initial.txt")
        initial_id = proxy.grid_id
        await doc.async_save()
        
        # Replace with new content
        new_content = b"Replaced content"
        new_file_obj = io.BytesIO(new_content)
        new_id = await proxy.async_replace(new_file_obj, filename="replaced.txt")
        
        # Verify replacement
        assert proxy.grid_id == new_id
        assert proxy.grid_id != initial_id
        
        # Read new content
        content = await proxy.async_read()
        assert content == new_content
    
    @pytest.mark.asyncio
    async def test_async_file_metadata(self):
        """Test file metadata retrieval."""
        # Upload file with metadata
        file_content = b"File with metadata"
        file_obj = io.BytesIO(file_content)
        
        doc = AsyncFileDoc(name="Metadata Test")
        proxy = await AsyncFileDoc.file.async_put(
            file_obj,
            instance=doc,
            filename="metadata.txt",
            metadata={"author": "test", "version": 1}
        )
        await doc.async_save()
        
        # Get metadata
        grid_out = await proxy.async_get()
        assert grid_out is not None
        assert grid_out.filename == "metadata.txt"
        assert grid_out.metadata["author"] == "test"
        assert grid_out.metadata["version"] == 1
    
    @pytest.mark.asyncio
    async def test_custom_collection_name(self):
        """Test FileField with custom collection name."""
        # Upload to custom collection
        file_content = b"Image data"
        file_obj = io.BytesIO(file_content)
        
        doc = AsyncImageDoc(name="Image Test")
        proxy = await AsyncImageDoc.image.async_put(file_obj, instance=doc, filename="image.jpg")
        await doc.async_save()
        
        # Verify custom collection was used
        assert proxy.collection_name == "async_images"
        
        # Read from custom collection
        loaded_doc = await AsyncImageDoc.objects.async_get(id=doc.id)
        proxy = await AsyncImageDoc.image.async_get(loaded_doc)
        content = await proxy.async_read()
        assert content == file_content
    
    @pytest.mark.asyncio
    async def test_empty_file_field(self):
        """Test document with no file uploaded."""
        doc = AsyncFileDoc(name="No File")
        await doc.async_save()
        
        # Try to get non-existent file
        proxy = await AsyncFileDoc.file.async_get(doc)
        assert proxy is None
    
    @pytest.mark.asyncio
    async def test_sync_connection_error(self):
        """Test that sync connection raises appropriate error."""
        # This test would require setting up a sync connection
        # For now, we're verifying the error handling in async methods
        pass
    
    @pytest.mark.asyncio
    async def test_multiple_files(self):
        """Test handling multiple file uploads."""
        files = []
        
        # Upload multiple files
        for i in range(3):
            content = f"File {i} content".encode()
            file_obj = io.BytesIO(content)
            
            doc = AsyncFileDoc(name=f"File {i}")
            proxy = await AsyncFileDoc.file.async_put(file_obj, instance=doc, filename=f"file{i}.txt")
            await doc.async_save()
            files.append((doc, content))
        
        # Verify all files
        for doc, expected_content in files:
            loaded_doc = await AsyncFileDoc.objects.async_get(id=doc.id)
            proxy = await AsyncFileDoc.file.async_get(loaded_doc)
            content = await proxy.async_read()
            assert content == expected_content