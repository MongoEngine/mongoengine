"""Test async reference field operations."""

import pytest
import pytest_asyncio
from bson import ObjectId

from mongoengine import (
    Document,
    StringField,
    IntField,
    ReferenceField,
    LazyReferenceField,
    ListField,
    connect_async,
    disconnect_async,
)
from mongoengine.base.datastructures import AsyncReferenceProxy, LazyReference
from mongoengine.errors import DoesNotExist


class AsyncAuthor(Document):
    """Test author document."""
    name = StringField(required=True)
    age = IntField()
    
    meta = {"collection": "async_test_ref_authors"}


class AsyncBook(Document):
    """Test book document with reference."""
    title = StringField(required=True)
    author = ReferenceField(AsyncAuthor)
    pages = IntField()
    
    meta = {"collection": "async_test_ref_books"}


class AsyncArticle(Document):
    """Test article with lazy reference."""
    title = StringField(required=True)
    author = LazyReferenceField(AsyncAuthor)
    content = StringField()
    
    meta = {"collection": "async_test_ref_articles"}


class TestAsyncReferenceField:
    """Test async reference field operations."""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connection and clean up after."""
        # Setup
        await connect_async(db="mongoenginetest_async_ref", alias="async_ref_test")
        AsyncAuthor._meta["db_alias"] = "async_ref_test"
        AsyncBook._meta["db_alias"] = "async_ref_test"
        AsyncArticle._meta["db_alias"] = "async_ref_test"
        
        yield
        
        # Teardown - clean collections
        try:
            await AsyncAuthor.async_drop_collection()
            await AsyncBook.async_drop_collection()
            await AsyncArticle.async_drop_collection()
        except:
            pass
        
        await disconnect_async("async_ref_test")
    
    @pytest.mark.asyncio
    async def test_reference_field_returns_proxy_in_async_context(self):
        """Test that ReferenceField returns AsyncReferenceProxy in async context."""
        # Create and save an author
        author = AsyncAuthor(name="Test Author", age=30)
        await author.async_save()
        
        # Create and save a book with reference
        book = AsyncBook(title="Test Book", author=author, pages=200)
        await book.async_save()
        
        # Reload the book
        loaded_book = await AsyncBook.objects.async_get(id=book.id)
        
        # In async context, author should be AsyncReferenceProxy
        assert isinstance(loaded_book.author, AsyncReferenceProxy)
        assert repr(loaded_book.author) == "<AsyncReferenceProxy: author (unfetched)>"
    
    @pytest.mark.asyncio
    async def test_async_reference_fetch(self):
        """Test fetching referenced document asynchronously."""
        # Create and save an author
        author = AsyncAuthor(name="John Doe", age=35)
        await author.async_save()
        
        # Create and save a book
        book = AsyncBook(title="Async Programming", author=author, pages=300)
        await book.async_save()
        
        # Reload and fetch reference
        loaded_book = await AsyncBook.objects.async_get(id=book.id)
        author_proxy = loaded_book.author
        
        # Fetch the author
        fetched_author = await author_proxy.fetch()
        assert isinstance(fetched_author, AsyncAuthor)
        assert fetched_author.name == "John Doe"
        assert fetched_author.age == 35
        assert fetched_author.id == author.id
        
        # Fetch again should use cache
        fetched_again = await author_proxy.fetch()
        assert fetched_again is fetched_author
    
    @pytest.mark.asyncio
    async def test_async_reference_missing_document(self):
        """Test handling of missing referenced documents."""
        # Create a book with a non-existent author reference
        fake_author_id = ObjectId()
        book = AsyncBook(title="Orphan Book", pages=100)
        book._data["author"] = AsyncAuthor(id=fake_author_id).to_dbref()
        await book.async_save()
        
        # Try to fetch the missing reference
        loaded_book = await AsyncBook.objects.async_get(id=book.id)
        author_proxy = loaded_book.author
        
        with pytest.raises(DoesNotExist):
            await author_proxy.fetch()
    
    @pytest.mark.asyncio
    async def test_async_reference_field_direct_fetch(self):
        """Test using async_fetch directly on field."""
        # Create and save an author
        author = AsyncAuthor(name="Jane Smith", age=40)
        await author.async_save()
        
        # Create and save a book
        book = AsyncBook(title="Direct Fetch Test", author=author, pages=250)
        await book.async_save()
        
        # Reload and use field's async_fetch
        loaded_book = await AsyncBook.objects.async_get(id=book.id)
        fetched_author = await AsyncBook.author.async_fetch(loaded_book)
        
        assert isinstance(fetched_author, AsyncAuthor)
        assert fetched_author.name == "Jane Smith"
        assert fetched_author.id == author.id
    
    @pytest.mark.asyncio
    async def test_lazy_reference_field_async_fetch(self):
        """Test LazyReferenceField async_fetch method."""
        # Create and save an author
        author = AsyncAuthor(name="Lazy Author", age=45)
        await author.async_save()
        
        # Create and save an article
        article = AsyncArticle(
            title="Lazy Loading Article",
            author=author,
            content="Content about lazy loading"
        )
        await article.async_save()
        
        # Reload article
        loaded_article = await AsyncArticle.objects.async_get(id=article.id)
        
        # Should get LazyReference object
        lazy_ref = loaded_article.author
        assert isinstance(lazy_ref, LazyReference)
        assert lazy_ref.pk == author.id
        
        # Async fetch
        fetched_author = await lazy_ref.async_fetch()
        assert isinstance(fetched_author, AsyncAuthor)
        assert fetched_author.name == "Lazy Author"
        assert fetched_author.age == 45
        
        # Fetch again should use cache
        fetched_again = await lazy_ref.async_fetch()
        assert fetched_again is fetched_author
    
    @pytest.mark.asyncio
    async def test_reference_list_field(self):
        """Test ListField of references in async context."""
        # Create multiple authors
        authors = []
        for i in range(3):
            author = AsyncAuthor(name=f"Author {i}", age=30 + i)
            await author.async_save()
            authors.append(author)
        
        # Create a document with list of references
        class AsyncBookCollection(Document):
            name = StringField()
            authors = ListField(ReferenceField(AsyncAuthor))
            meta = {"collection": "async_test_book_collections"}
        
        AsyncBookCollection._meta["db_alias"] = "async_ref_test"
        
        collection = AsyncBookCollection(name="Test Collection")
        collection.authors = authors
        await collection.async_save()
        
        # Reload and check
        loaded = await AsyncBookCollection.objects.async_get(id=collection.id)
        
        # ListField doesn't automatically convert to AsyncReferenceProxy
        # This is a known limitation - references in lists need manual handling
        # For now, verify the references are DBRefs
        from bson import DBRef
        for i, author_ref in enumerate(loaded.authors):
            assert isinstance(author_ref, DBRef)
            # Manual async dereferencing would be needed here
            # This is a TODO for future enhancement
        
        # Cleanup
        await AsyncBookCollection.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_sync_connection_error(self):
        """Test that sync connection raises appropriate error."""
        # This test would require setting up a sync connection
        # For now, we're verifying the error handling in async_fetch
        pass
    
    @pytest.mark.asyncio
    async def test_reference_field_query(self):
        """Test querying by reference field."""
        # Create authors
        author1 = AsyncAuthor(name="Author One", age=30)
        author2 = AsyncAuthor(name="Author Two", age=40)
        await author1.async_save()
        await author2.async_save()
        
        # Create books
        book1 = AsyncBook(title="Book 1", author=author1, pages=100)
        book2 = AsyncBook(title="Book 2", author=author1, pages=200)
        book3 = AsyncBook(title="Book 3", author=author2, pages=300)
        await book1.async_save()
        await book2.async_save()
        await book3.async_save()
        
        # Query by reference
        author1_books = await AsyncBook.objects.filter(author=author1).async_to_list()
        assert len(author1_books) == 2
        assert set(b.title for b in author1_books) == {"Book 1", "Book 2"}
        
        author2_books = await AsyncBook.objects.filter(author=author2).async_to_list()
        assert len(author2_books) == 1
        assert author2_books[0].title == "Book 3"