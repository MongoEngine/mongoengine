"""Test async document operations."""

import pytest
import pytest_asyncio
import pymongo
from bson import ObjectId

from mongoengine import (
    Document,
    EmbeddedDocument,
    StringField,
    IntField,
    EmbeddedDocumentField,
    ListField,
    ReferenceField,
    connect_async,
    disconnect_async,
)
from mongoengine.errors import InvalidDocumentError, NotUniqueError, OperationError


class Address(EmbeddedDocument):
    """Test embedded document."""
    street = StringField()
    city = StringField()
    country = StringField()


class Person(Document):
    """Test document."""
    name = StringField(required=True)
    age = IntField()
    address = EmbeddedDocumentField(Address)
    
    meta = {"collection": "async_test_person"}


class Post(Document):
    """Test document with reference."""
    title = StringField(required=True)
    author = ReferenceField(Person)
    
    meta = {"collection": "async_test_post"}


class TestAsyncDocument:
    """Test async document operations."""

    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connection and clean up after."""
        # Setup
        await connect_async(db="mongoenginetest_async", alias="async_doc_test")
        Person._meta["db_alias"] = "async_doc_test"
        Post._meta["db_alias"] = "async_doc_test"
        
        yield
        
        # Teardown - clean collections
        try:
            await Person.async_drop_collection()
            await Post.async_drop_collection()
        except:
            pass
        
        await disconnect_async("async_doc_test")

    @pytest.mark.asyncio
    async def test_async_save_basic(self):
        """Test basic async save operation."""
        # Create and save document
        person = Person(name="John Doe", age=30)
        saved_person = await person.async_save()
        
        # Verify save
        assert saved_person is person
        assert person.id is not None
        assert isinstance(person.id, ObjectId)
        
        # Verify in database
        collection = await Person._async_get_collection()
        doc = await collection.find_one({"_id": person.id})
        assert doc is not None
        assert doc["name"] == "John Doe"
        assert doc["age"] == 30

    @pytest.mark.asyncio
    async def test_async_save_with_embedded(self):
        """Test async save with embedded document."""
        # Create document with embedded
        address = Address(street="123 Main St", city="New York", country="USA")
        person = Person(name="Jane Doe", age=25, address=address)
        
        await person.async_save()
        
        # Verify in database
        collection = await Person._async_get_collection()
        doc = await collection.find_one({"_id": person.id})
        assert doc is not None
        assert doc["address"]["street"] == "123 Main St"
        assert doc["address"]["city"] == "New York"

    @pytest.mark.asyncio
    async def test_async_save_update(self):
        """Test async save for updating existing document."""
        # Create and save
        person = Person(name="Bob", age=40)
        await person.async_save()
        original_id = person.id
        
        # Update and save again
        person.age = 41
        await person.async_save()
        
        # Verify update
        assert person.id == original_id  # ID should not change
        
        collection = await Person._async_get_collection()
        doc = await collection.find_one({"_id": person.id})
        assert doc["age"] == 41

    @pytest.mark.asyncio
    async def test_async_save_force_insert(self):
        """Test async save with force_insert."""
        # Create with specific ID
        person = Person(name="Alice", age=35)
        person.id = ObjectId()
        
        # Save with force_insert
        await person.async_save(force_insert=True)
        
        # Try to save again with force_insert (should fail)
        with pytest.raises(NotUniqueError):
            await person.async_save(force_insert=True)

    @pytest.mark.asyncio
    async def test_async_delete(self):
        """Test async delete operation."""
        # Create and save
        person = Person(name="Delete Me", age=50)
        await person.async_save()
        person_id = person.id
        
        # Delete
        await person.async_delete()
        
        # Verify deletion
        collection = await Person._async_get_collection()
        doc = await collection.find_one({"_id": person_id})
        assert doc is None

    @pytest.mark.asyncio
    async def test_async_delete_unsaved(self):
        """Test async delete on unsaved document (should not raise error)."""
        person = Person(name="Never Saved")
        # Should not raise error even if document was never saved
        await person.async_delete()

    @pytest.mark.asyncio
    async def test_async_reload(self):
        """Test async reload operation."""
        # Create and save
        person = Person(name="Original Name", age=30)
        await person.async_save()
        
        # Modify in database directly
        collection = await Person._async_get_collection()
        await collection.update_one(
            {"_id": person.id},
            {"$set": {"name": "Updated Name", "age": 31}}
        )
        
        # Reload
        await person.async_reload()
        
        # Verify reload
        assert person.name == "Updated Name"
        assert person.age == 31

    @pytest.mark.asyncio
    async def test_async_reload_specific_fields(self):
        """Test async reload with specific fields."""
        # Create and save
        person = Person(name="Test Person", age=25)
        await person.async_save()
        
        # Modify in database
        collection = await Person._async_get_collection()
        await collection.update_one(
            {"_id": person.id},
            {"$set": {"name": "New Name", "age": 26}}
        )
        
        # Reload only name
        await person.async_reload("name")
        
        # Verify partial reload
        assert person.name == "New Name"
        assert person.age == 25  # Should not be updated

    @pytest.mark.asyncio
    async def test_async_cascade_save(self):
        """Test async cascade save with references."""
        # For now, test cascade save with already saved reference
        # TODO: Implement proper cascade save for unsaved references
        
        # Create and save author first
        author = Person(name="Author", age=45)
        await author.async_save()
        
        # Create post with saved author
        post = Post(title="Test Post", author=author)
        
        # Save post with cascade
        await post.async_save(cascade=True)
        
        # Verify both are saved
        assert post.id is not None
        assert author.id is not None
        
        # Verify in database
        author_collection = await Person._async_get_collection()
        author_doc = await author_collection.find_one({"_id": author.id})
        assert author_doc is not None
        
        post_collection = await Post._async_get_collection()
        post_doc = await post_collection.find_one({"_id": post.id})
        assert post_doc is not None
        assert post_doc["author"] == author.id

    @pytest.mark.asyncio
    async def test_async_ensure_indexes(self):
        """Test async index creation."""
        # Define a document with indexes
        class IndexedDoc(Document):
            name = StringField()
            email = StringField()
            
            meta = {
                "collection": "async_indexed",
                "indexes": [
                    "name",
                    ("email", "-name"),  # Compound index
                ],
                "db_alias": "async_doc_test"
            }
        
        # Ensure indexes
        await IndexedDoc.async_ensure_indexes()
        
        # Verify indexes exist
        collection = await IndexedDoc._async_get_collection()
        indexes = await collection.index_information()
        
        # Should have _id index plus our custom indexes
        assert len(indexes) >= 3
        
        # Clean up
        await IndexedDoc.async_drop_collection()

    @pytest.mark.asyncio
    async def test_async_save_validation(self):
        """Test validation during async save."""
        # Try to save without required field
        person = Person(age=30)  # Missing required 'name'
        
        from mongoengine.errors import ValidationError
        with pytest.raises(ValidationError):
            await person.async_save()

    @pytest.mark.asyncio
    async def test_sync_methods_with_async_connection(self):
        """Test that sync methods raise error with async connection."""
        person = Person(name="Test", age=30)
        
        # Try to use sync save
        with pytest.raises(RuntimeError) as exc_info:
            person.save()
        assert "async" in str(exc_info.value).lower()
        
        # Try to use sync delete
        with pytest.raises(RuntimeError) as exc_info:
            person.delete()
        assert "async" in str(exc_info.value).lower()
        
        # Try to use sync reload
        with pytest.raises(RuntimeError) as exc_info:
            person.reload()
        assert "async" in str(exc_info.value).lower()