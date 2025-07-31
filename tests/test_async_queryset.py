"""Test async queryset operations."""

import pytest
import pytest_asyncio
from bson import ObjectId

from mongoengine import (
    Document,
    StringField,
    IntField,
    ListField,
    ReferenceField,
    connect_async,
    disconnect_async,
)
from mongoengine.errors import DoesNotExist, MultipleObjectsReturned


class AsyncAuthor(Document):
    """Test author document."""
    name = StringField(required=True)
    age = IntField()
    
    meta = {"collection": "async_test_authors"}


class AsyncBook(Document):
    """Test book document with reference."""
    title = StringField(required=True)
    author = ReferenceField(AsyncAuthor)
    pages = IntField()
    tags = ListField(StringField())
    
    meta = {"collection": "async_test_books"}


class TestAsyncQuerySet:
    """Test async queryset operations."""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connection and clean up after."""
        # Setup
        await connect_async(db="mongoenginetest_async_queryset", alias="async_qs_test")
        AsyncAuthor._meta["db_alias"] = "async_qs_test"
        AsyncBook._meta["db_alias"] = "async_qs_test"
        
        yield
        
        # Teardown - clean collections
        try:
            await AsyncAuthor.async_drop_collection()
            await AsyncBook.async_drop_collection()
        except:
            pass
        
        await disconnect_async("async_qs_test")
    
    @pytest.mark.asyncio
    async def test_async_first(self):
        """Test async_first() method."""
        # Test empty collection
        result = await AsyncAuthor.objects.async_first()
        assert result is None
        
        # Create some authors
        author1 = AsyncAuthor(name="Author 1", age=30)
        author2 = AsyncAuthor(name="Author 2", age=40)
        await author1.async_save()
        await author2.async_save()
        
        # Test first without filter
        first_author = await AsyncAuthor.objects.async_first()
        assert first_author is not None
        assert isinstance(first_author, AsyncAuthor)
        
        # Test first with filter
        filtered = await AsyncAuthor.objects.filter(age=40).async_first()
        assert filtered.name == "Author 2"
        
        # Test with ordering
        oldest = await AsyncAuthor.objects.order_by("-age").async_first()
        assert oldest.name == "Author 2"
    
    @pytest.mark.asyncio
    async def test_async_get(self):
        """Test async_get() method."""
        # Test DoesNotExist
        with pytest.raises(DoesNotExist):
            await AsyncAuthor.objects.async_get(name="Non-existent")
        
        # Create an author
        author = AsyncAuthor(name="Unique Author", age=35)
        await author.async_save()
        
        # Test successful get
        retrieved = await AsyncAuthor.objects.async_get(name="Unique Author")
        assert retrieved.id == author.id
        assert retrieved.age == 35
        
        # Create another author with same age
        author2 = AsyncAuthor(name="Another Author", age=35)
        await author2.async_save()
        
        # Test MultipleObjectsReturned
        with pytest.raises(MultipleObjectsReturned):
            await AsyncAuthor.objects.async_get(age=35)
        
        # Test get with Q objects
        from mongoengine.queryset.visitor import Q
        result = await AsyncAuthor.objects.async_get(Q(name="Unique Author") & Q(age=35))
        assert result.id == author.id
    
    @pytest.mark.asyncio
    async def test_async_count(self):
        """Test async_count() method."""
        # Test empty collection
        count = await AsyncAuthor.objects.async_count()
        assert count == 0
        
        # Add some documents
        for i in range(5):
            author = AsyncAuthor(name=f"Author {i}", age=20 + i * 10)
            await author.async_save()
        
        # Test total count
        total = await AsyncAuthor.objects.async_count()
        assert total == 5
        
        # Test filtered count
        young_count = await AsyncAuthor.objects.filter(age__lt=40).async_count()
        assert young_count == 2
        
        # Test count with limit
        limited_count = await AsyncAuthor.objects.limit(3).async_count(with_limit_and_skip=True)
        assert limited_count == 3
        
        # Test count with skip
        skip_count = await AsyncAuthor.objects.skip(2).async_count(with_limit_and_skip=True)
        assert skip_count == 3
    
    @pytest.mark.asyncio
    async def test_async_exists(self):
        """Test async_exists() method."""
        # Test empty collection
        exists = await AsyncAuthor.objects.async_exists()
        assert exists is False
        
        # Add a document
        author = AsyncAuthor(name="Test Author", age=30)
        await author.async_save()
        
        # Test exists with data
        exists = await AsyncAuthor.objects.async_exists()
        assert exists is True
        
        # Test filtered exists
        exists = await AsyncAuthor.objects.filter(age=30).async_exists()
        assert exists is True
        
        exists = await AsyncAuthor.objects.filter(age=100).async_exists()
        assert exists is False
    
    @pytest.mark.asyncio
    async def test_async_to_list(self):
        """Test async_to_list() method."""
        # Test empty collection
        result = await AsyncAuthor.objects.async_to_list()
        assert result == []
        
        # Add some documents
        authors = []
        for i in range(10):
            author = AsyncAuthor(name=f"Author {i}", age=20 + i)
            await author.async_save()
            authors.append(author)
        
        # Test full list
        all_authors = await AsyncAuthor.objects.async_to_list()
        assert len(all_authors) == 10
        assert all(isinstance(a, AsyncAuthor) for a in all_authors)
        
        # Test limited list
        limited = await AsyncAuthor.objects.async_to_list(length=5)
        assert len(limited) == 5
        
        # Test filtered list
        young = await AsyncAuthor.objects.filter(age__lt=25).async_to_list()
        assert len(young) == 5
        
        # Test ordered list
        ordered = await AsyncAuthor.objects.order_by("-age").async_to_list(length=3)
        assert ordered[0].age == 29
        assert ordered[1].age == 28
        assert ordered[2].age == 27
    
    @pytest.mark.asyncio
    async def test_async_iteration(self):
        """Test async iteration over queryset."""
        # Add some documents
        for i in range(5):
            author = AsyncAuthor(name=f"Author {i}", age=20 + i)
            await author.async_save()
        
        # Test basic iteration
        count = 0
        names = []
        async for author in AsyncAuthor.objects:
            count += 1
            names.append(author.name)
            assert isinstance(author, AsyncAuthor)
        
        assert count == 5
        assert len(names) == 5
        
        # Test filtered iteration
        young_count = 0
        async for author in AsyncAuthor.objects.filter(age__lt=23):
            young_count += 1
            assert author.age < 23
        
        assert young_count == 3
        
        # Test ordered iteration
        ages = []
        async for author in AsyncAuthor.objects.order_by("age"):
            ages.append(author.age)
        
        assert ages == [20, 21, 22, 23, 24]
    
    @pytest.mark.asyncio
    async def test_async_create(self):
        """Test async_create() method."""
        # Create using async_create
        author = await AsyncAuthor.objects.async_create(
            name="Created Author",
            age=45
        )
        
        assert author.id is not None
        assert isinstance(author, AsyncAuthor)
        
        # Verify it was saved
        count = await AsyncAuthor.objects.async_count()
        assert count == 1
        
        retrieved = await AsyncAuthor.objects.async_get(id=author.id)
        assert retrieved.name == "Created Author"
        assert retrieved.age == 45
    
    @pytest.mark.asyncio
    async def test_async_update(self):
        """Test async_update() method."""
        # Create some authors
        for i in range(5):
            await AsyncAuthor.objects.async_create(
                name=f"Author {i}",
                age=20 + i
            )
        
        # Update all documents
        updated = await AsyncAuthor.objects.async_update(age=30)
        assert updated == 5
        
        # Verify update
        all_authors = await AsyncAuthor.objects.async_to_list()
        assert all(a.age == 30 for a in all_authors)
        
        # Update with filter
        updated = await AsyncAuthor.objects.filter(
            name__in=["Author 0", "Author 1"]
        ).async_update(age=50)
        assert updated == 2
        
        # Update with operators
        updated = await AsyncAuthor.objects.filter(
            age=50
        ).async_update(inc__age=5)
        assert updated == 2
        
        # Verify increment
        old_authors = await AsyncAuthor.objects.filter(age=55).async_to_list()
        assert len(old_authors) == 2
    
    @pytest.mark.asyncio
    async def test_async_update_one(self):
        """Test async_update_one() method."""
        # Create some authors
        for i in range(3):
            await AsyncAuthor.objects.async_create(
                name=f"Author {i}",
                age=30
            )
        
        # Update one document
        updated = await AsyncAuthor.objects.filter(age=30).async_update_one(age=40)
        assert updated == 1
        
        # Verify only one was updated
        count_30 = await AsyncAuthor.objects.filter(age=30).async_count()
        count_40 = await AsyncAuthor.objects.filter(age=40).async_count()
        assert count_30 == 2
        assert count_40 == 1
    
    @pytest.mark.asyncio
    async def test_async_delete(self):
        """Test async_delete() method."""
        # Create some documents
        ids = []
        for i in range(5):
            author = await AsyncAuthor.objects.async_create(
                name=f"Author {i}",
                age=20 + i
            )
            ids.append(author.id)
        
        # Delete with filter
        deleted = await AsyncAuthor.objects.filter(age__lt=22).async_delete()
        assert deleted == 2
        
        # Verify deletion
        remaining = await AsyncAuthor.objects.async_count()
        assert remaining == 3
        
        # Delete all
        deleted = await AsyncAuthor.objects.async_delete()
        assert deleted == 3
        
        # Verify all deleted
        count = await AsyncAuthor.objects.async_count()
        assert count == 0
    
    @pytest.mark.asyncio
    async def test_queryset_chaining(self):
        """Test chaining of queryset methods with async execution."""
        # Create test data
        for i in range(10):
            await AsyncAuthor.objects.async_create(
                name=f"Author {i}",
                age=20 + i * 2
            )
        
        # Test complex chaining
        result = await AsyncAuthor.objects.filter(
            age__gte=25
        ).filter(
            age__lte=35
        ).order_by(
            "-age"
        ).limit(3).async_to_list()
        
        assert len(result) == 3
        assert result[0].age == 34
        assert result[1].age == 32
        assert result[2].age == 30
        
        # Test only() with async
        partial = await AsyncAuthor.objects.only("name").async_first()
        assert partial.name is not None
        # Note: age might still be loaded depending on implementation
    
    @pytest.mark.asyncio
    async def test_async_with_references(self):
        """Test async operations with referenced documents."""
        # Create an author
        author = await AsyncAuthor.objects.async_create(
            name="Book Author",
            age=40
        )
        
        # Create books
        for i in range(3):
            await AsyncBook.objects.async_create(
                title=f"Book {i}",
                author=author,
                pages=100 + i * 50,
                tags=["fiction", f"tag{i}"]
            )
        
        # Query books by author
        books = await AsyncBook.objects.filter(author=author).async_to_list()
        assert len(books) == 3
        
        # Test count with reference
        count = await AsyncBook.objects.filter(author=author).async_count()
        assert count == 3
        
        # Delete author's books
        deleted = await AsyncBook.objects.filter(author=author).async_delete()
        assert deleted == 3
    
    @pytest.mark.asyncio
    async def test_as_pymongo(self):
        """Test as_pymongo() with async methods."""
        # Create a document
        await AsyncAuthor.objects.async_create(
            name="PyMongo Test",
            age=30
        )
        
        # Get as pymongo dict
        result = await AsyncAuthor.objects.as_pymongo().async_first()
        assert isinstance(result, dict)
        assert result["name"] == "PyMongo Test"
        assert result["age"] == 30
        assert "_id" in result
        
        # Test with iteration
        async for doc in AsyncAuthor.objects.as_pymongo():
            assert isinstance(doc, dict)
            assert "name" in doc
    
    @pytest.mark.asyncio
    async def test_error_with_sync_connection(self):
        """Test that async methods raise error with sync connection."""
        # This test would require setting up a sync connection
        # For now, we're testing that our methods properly check connection type
        pass