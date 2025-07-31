"""Test async context managers."""

import pytest
import pytest_asyncio

from mongoengine import (
    Document,
    StringField,
    ReferenceField,
    ListField,
    connect_async,
    disconnect_async,
    register_connection,
)
from mongoengine.async_context_managers import (
    async_switch_db,
    async_switch_collection,
    async_no_dereference,
)
from mongoengine.base.datastructures import AsyncReferenceProxy
from mongoengine.errors import NotUniqueError, OperationError


class TestAsyncContextManagers:
    """Test async context manager operations."""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connections and clean up after."""
        # Setup multiple database connections
        await connect_async(db="mongoenginetest_async_ctx", alias="default")
        await connect_async(db="mongoenginetest_async_ctx2", alias="testdb-1")
        
        yield
        
        # Cleanup
        await disconnect_async("default")
        await disconnect_async("testdb-1")
    
    @pytest.mark.asyncio
    async def test_async_switch_db(self):
        """Test async_switch_db context manager."""
        class Group(Document):
            name = StringField(required=True)
            meta = {"collection": "test_switch_db_groups"}
        
        try:
            # Save to default database
            group1 = Group(name="default_group")
            await group1.async_save()
            
            # Verify it's in default db
            assert await Group.objects.async_count() == 1
            
            # Switch to testdb-1
            async with async_switch_db(Group, "testdb-1") as GroupInTestDb:
                # Should be empty in testdb-1
                assert await GroupInTestDb.objects.async_count() == 0
                
                # Save to testdb-1
                group2 = GroupInTestDb(name="testdb_group")
                await group2.async_save()
                
                # Verify it's saved
                assert await GroupInTestDb.objects.async_count() == 1
            
            # Back in default db
            assert await Group.objects.async_count() == 1
            groups = await Group.objects.async_to_list()
            assert groups[0].name == "default_group"
            
            # Check testdb-1 still has its document
            async with async_switch_db(Group, "testdb-1") as GroupInTestDb:
                assert await GroupInTestDb.objects.async_count() == 1
                groups = await GroupInTestDb.objects.async_to_list()
                assert groups[0].name == "testdb_group"
            
        finally:
            # Cleanup both databases
            await Group.async_drop_collection()
            async with async_switch_db(Group, "testdb-1") as GroupInTestDb:
                await GroupInTestDb.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_switch_collection(self):
        """Test async_switch_collection context manager."""
        class Person(Document):
            name = StringField(required=True)
            meta = {"collection": "test_switch_coll_people"}
        
        try:
            # Save to default collection
            person1 = Person(name="person_in_people")
            await person1.async_save()
            
            # Verify it's in default collection
            assert await Person.objects.async_count() == 1
            
            # Switch to backup collection
            async with async_switch_collection(Person, "people_backup") as PersonBackup:
                # Should be empty in backup collection
                assert await PersonBackup.objects.async_count() == 0
                
                # Save to backup collection
                person2 = PersonBackup(name="person_in_backup")
                await person2.async_save()
                
                # Verify it's saved
                assert await PersonBackup.objects.async_count() == 1
            
            # Back in default collection
            assert await Person.objects.async_count() == 1
            people = await Person.objects.async_to_list()
            assert people[0].name == "person_in_people"
            
            # Check backup collection still has its document
            async with async_switch_collection(Person, "people_backup") as PersonBackup:
                assert await PersonBackup.objects.async_count() == 1
                people = await PersonBackup.objects.async_to_list()
                assert people[0].name == "person_in_backup"
            
        finally:
            # Cleanup both collections
            await Person.async_drop_collection()
            async with async_switch_collection(Person, "people_backup") as PersonBackup:
                await PersonBackup.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_no_dereference(self):
        """Test async_no_dereference context manager."""
        class Author(Document):
            name = StringField(required=True)
            meta = {"collection": "test_no_deref_authors"}
        
        class Book(Document):
            title = StringField(required=True)
            author = ReferenceField(Author)
            meta = {"collection": "test_no_deref_books"}
        
        try:
            # Create test data
            author = Author(name="Test Author")
            await author.async_save()
            
            book = Book(title="Test Book", author=author)
            await book.async_save()
            
            # Normal behavior - returns AsyncReferenceProxy
            loaded_book = await Book.objects.async_first()
            assert isinstance(loaded_book.author, AsyncReferenceProxy)
            
            # Fetch the author
            fetched_author = await loaded_book.author.fetch()
            assert fetched_author.name == "Test Author"
            
            # With no_dereference - should not dereference
            async with async_no_dereference(Book):
                loaded_book = await Book.objects.async_first()
                # Should get DBRef or similar, not AsyncReferenceProxy
                from bson import DBRef
                assert isinstance(loaded_book.author, (DBRef, type(None)))
                # If it's a DBRef, check it points to the right document
                if isinstance(loaded_book.author, DBRef):
                    assert loaded_book.author.id == author.id
            
            # Back to normal behavior
            loaded_book = await Book.objects.async_first()
            assert isinstance(loaded_book.author, AsyncReferenceProxy)
            
        finally:
            # Cleanup
            await Author.async_drop_collection()
            await Book.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_nested_context_managers(self):
        """Test nested async context managers."""
        class Data(Document):
            name = StringField(required=True)
            value = StringField()
            meta = {"collection": "test_nested_data"}
        
        try:
            # Create data in default db/collection
            data1 = Data(name="default", value="in_default")
            await data1.async_save()
            
            # Nest db and collection switches
            async with async_switch_db(Data, "testdb-1") as DataInTestDb:
                # Save in testdb-1
                data2 = DataInTestDb(name="testdb1", value="in_testdb1")
                await data2.async_save()
                
                # Switch collection within testdb-1
                async with async_switch_collection(DataInTestDb, "data_archive") as DataArchive:
                    # Save in testdb-1/data_archive
                    data3 = DataArchive(name="archive", value="in_archive")
                    await data3.async_save()
                    
                    assert await DataArchive.objects.async_count() == 1
                
                # Back to testdb-1/default collection
                assert await DataInTestDb.objects.async_count() == 1
            
            # Back to default db
            assert await Data.objects.async_count() == 1
            
            # Verify all data exists in correct places
            data = await Data.objects.async_first()
            assert data.name == "default"
            
            async with async_switch_db(Data, "testdb-1") as DataInTestDb:
                data = await DataInTestDb.objects.async_first()
                assert data.name == "testdb1"
                
                async with async_switch_collection(DataInTestDb, "data_archive") as DataArchive:
                    data = await DataArchive.objects.async_first()
                    assert data.name == "archive"
            
        finally:
            # Cleanup all collections
            await Data.async_drop_collection()
            async with async_switch_db(Data, "testdb-1") as DataInTestDb:
                await DataInTestDb.async_drop_collection()
                async with async_switch_collection(DataInTestDb, "data_archive") as DataArchive:
                    await DataArchive.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_exception_handling(self):
        """Test that context managers properly restore state on exceptions."""
        class Item(Document):
            name = StringField(required=True)
            value = StringField()
            meta = {"collection": "test_exception_items"}
        
        try:
            # Save an item
            item1 = Item(name="test_item", value="original")
            await item1.async_save()
            
            original_db_alias = Item._meta.get("db_alias", "default")
            
            # Test exception in switch_db
            with pytest.raises(RuntimeError):
                async with async_switch_db(Item, "testdb-1"):
                    # Create some operation
                    item2 = Item(name="testdb_item")
                    await item2.async_save()
                    # Raise an exception
                    raise RuntimeError("Test exception")
            
            # Verify db_alias is restored
            assert Item._meta.get("db_alias") == original_db_alias
            
            # Verify we're back in the original database
            items = await Item.objects.async_to_list()
            assert len(items) == 1
            assert items[0].name == "test_item"
            
            original_collection_name = Item._get_collection_name()
            
            # Test exception in switch_collection
            with pytest.raises(RuntimeError):
                async with async_switch_collection(Item, "items_backup"):
                    # Create some operation
                    item3 = Item(name="backup_item")
                    await item3.async_save()
                    # Raise an exception
                    raise RuntimeError("Test exception")
            
            # Verify collection name is restored
            assert Item._get_collection_name() == original_collection_name
            
            # Verify we're back in the original collection
            items = await Item.objects.async_to_list()
            assert len(items) == 1
            assert items[0].name == "test_item"
            
        finally:
            # Cleanup
            await Item.async_drop_collection()
            async with async_switch_db(Item, "testdb-1"):
                await Item.async_drop_collection()
            async with async_switch_collection(Item, "items_backup"):
                await Item.async_drop_collection()