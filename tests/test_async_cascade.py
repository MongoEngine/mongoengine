"""Test async cascade operations."""

import pytest
import pytest_asyncio

from mongoengine import (
    Document,
    StringField,
    ReferenceField,
    ListField,
    connect_async,
    disconnect_async,
    CASCADE,
    NULLIFY,
    PULL,
    DENY,
)
from mongoengine.errors import OperationError


class TestAsyncCascadeOperations:
    """Test async cascade delete operations."""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connection and clean up after."""
        # Setup
        await connect_async(db="mongoenginetest_async_cascade", alias="async_cascade_test")
        
        yield
        
        await disconnect_async("async_cascade_test")
    
    @pytest.mark.asyncio
    async def test_cascade_delete(self):
        """Test CASCADE delete rule - referenced documents are deleted."""
        # Define documents for this test
        class AsyncAuthor(Document):
            name = StringField(required=True)
            meta = {"collection": "test_cascade_authors", "db_alias": "async_cascade_test"}
        
        class AsyncBook(Document):
            title = StringField(required=True)
            author = ReferenceField(AsyncAuthor, reverse_delete_rule=CASCADE)
            meta = {"collection": "test_cascade_books", "db_alias": "async_cascade_test"}
        
        try:
            # Create author and books
            author = AsyncAuthor(name="John Doe")
            await author.async_save()
            
            book1 = AsyncBook(title="Book 1", author=author)
            book2 = AsyncBook(title="Book 2", author=author)
            await book1.async_save()
            await book2.async_save()
            
            # Verify setup
            assert await AsyncAuthor.objects.async_count() == 1
            assert await AsyncBook.objects.async_count() == 2
            
            # Delete author - should cascade to books
            await author.async_delete()
            
            # Verify cascade deletion
            assert await AsyncAuthor.objects.async_count() == 0
            assert await AsyncBook.objects.async_count() == 0
            
        finally:
            # Cleanup
            await AsyncAuthor.async_drop_collection()
            await AsyncBook.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_nullify_delete(self):
        """Test NULLIFY delete rule - references are set to null."""
        # Define documents for this test
        class AsyncAuthor(Document):
            name = StringField(required=True)
            meta = {"collection": "test_nullify_authors", "db_alias": "async_cascade_test"}
        
        class AsyncArticle(Document):
            title = StringField(required=True)
            author = ReferenceField(AsyncAuthor, reverse_delete_rule=NULLIFY)
            meta = {"collection": "test_nullify_articles", "db_alias": "async_cascade_test"}
        
        try:
            # Create author and articles
            author = AsyncAuthor(name="Jane Smith")
            await author.async_save()
            
            article1 = AsyncArticle(title="Article 1", author=author)
            article2 = AsyncArticle(title="Article 2", author=author)
            await article1.async_save()
            await article2.async_save()
            
            # Delete author - should nullify references
            await author.async_delete()
            
            # Verify author is deleted but articles remain with null author
            assert await AsyncAuthor.objects.async_count() == 0
            assert await AsyncArticle.objects.async_count() == 2
            
            # Check that author fields are nullified
            article1_reloaded = await AsyncArticle.objects.async_get(id=article1.id)
            article2_reloaded = await AsyncArticle.objects.async_get(id=article2.id)
            assert article1_reloaded.author is None
            assert article2_reloaded.author is None
            
        finally:
            # Cleanup
            await AsyncAuthor.async_drop_collection()
            await AsyncArticle.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_pull_delete(self):
        """Test PULL delete rule - references are removed from lists."""
        # Define documents for this test
        class AsyncAuthor(Document):
            name = StringField(required=True)
            meta = {"collection": "test_pull_authors", "db_alias": "async_cascade_test"}
        
        class AsyncBlog(Document):
            name = StringField(required=True)
            authors = ListField(ReferenceField(AsyncAuthor, reverse_delete_rule=PULL))
            meta = {"collection": "test_pull_blogs", "db_alias": "async_cascade_test"}
        
        try:
            # Create authors
            author1 = AsyncAuthor(name="Author 1")
            author2 = AsyncAuthor(name="Author 2")
            author3 = AsyncAuthor(name="Author 3")
            await author1.async_save()
            await author2.async_save()
            await author3.async_save()
            
            # Create blog with multiple authors
            blog = AsyncBlog(name="Tech Blog", authors=[author1, author2, author3])
            await blog.async_save()
            
            # Delete one author - should be pulled from the list
            await author2.async_delete()
            
            # Verify author is deleted and removed from blog
            assert await AsyncAuthor.objects.async_count() == 2
            blog_reloaded = await AsyncBlog.objects.async_get(id=blog.id)
            assert len(blog_reloaded.authors) == 2
            author_ids = [a.id for a in blog_reloaded.authors]
            assert author1.id in author_ids
            assert author3.id in author_ids
            assert author2.id not in author_ids
            
        finally:
            # Cleanup
            await AsyncAuthor.async_drop_collection()
            await AsyncBlog.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_deny_delete(self):
        """Test DENY delete rule - deletion is prevented if references exist."""
        # Define documents for this test
        class AsyncAuthor(Document):
            name = StringField(required=True)
            meta = {"collection": "test_deny_authors", "db_alias": "async_cascade_test"}
        
        class AsyncReview(Document):
            content = StringField(required=True)
            author = ReferenceField(AsyncAuthor, reverse_delete_rule=DENY)
            meta = {"collection": "test_deny_reviews", "db_alias": "async_cascade_test"}
        
        try:
            # Create author and review
            author = AsyncAuthor(name="Reviewer")
            await author.async_save()
            
            review = AsyncReview(content="Great book!", author=author)
            await review.async_save()
            
            # Try to delete author - should be denied
            with pytest.raises(OperationError) as exc:
                await author.async_delete()
            
            assert "Could not delete document" in str(exc.value)
            assert "AsyncReview.author refers to it" in str(exc.value)
            
            # Verify nothing was deleted
            assert await AsyncAuthor.objects.async_count() == 1
            assert await AsyncReview.objects.async_count() == 1
            
            # Delete review first, then author should work
            await review.async_delete()
            await author.async_delete()
            
            assert await AsyncAuthor.objects.async_count() == 0
            assert await AsyncReview.objects.async_count() == 0
            
        finally:
            # Cleanup
            await AsyncAuthor.async_drop_collection()
            await AsyncReview.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_cascade_with_multiple_levels(self):
        """Test cascade delete with multiple levels of references."""
        # Define documents for this test
        class AsyncAuthor(Document):
            name = StringField(required=True)
            meta = {"collection": "test_multi_authors", "db_alias": "async_cascade_test"}
        
        class AsyncBook(Document):
            title = StringField(required=True)
            author = ReferenceField(AsyncAuthor, reverse_delete_rule=CASCADE)
            meta = {"collection": "test_multi_books", "db_alias": "async_cascade_test"}
        
        class AsyncArticle(Document):
            title = StringField(required=True)
            author = ReferenceField(AsyncAuthor, reverse_delete_rule=NULLIFY)
            meta = {"collection": "test_multi_articles", "db_alias": "async_cascade_test"}
        
        class AsyncBlog(Document):
            name = StringField(required=True)
            authors = ListField(ReferenceField(AsyncAuthor, reverse_delete_rule=PULL))
            meta = {"collection": "test_multi_blogs", "db_alias": "async_cascade_test"}
        
        try:
            # Create a more complex scenario
            author = AsyncAuthor(name="Multi-level Author")
            await author.async_save()
            
            # Create multiple books
            books = []
            for i in range(5):
                book = AsyncBook(title=f"Book {i}", author=author)
                await book.async_save()
                books.append(book)
            
            # Create articles  
            articles = []
            for i in range(3):
                article = AsyncArticle(title=f"Article {i}", author=author)
                await article.async_save()
                articles.append(article)
            
            # Create blog with author
            blog = AsyncBlog(name="Multi Blog", authors=[author])
            await blog.async_save()
            
            # Verify setup
            assert await AsyncAuthor.objects.async_count() == 1
            assert await AsyncBook.objects.async_count() == 5
            assert await AsyncArticle.objects.async_count() == 3
            assert await AsyncBlog.objects.async_count() == 1
            
            # Delete author - should trigger all rules
            await author.async_delete()
            
            # Verify results
            assert await AsyncAuthor.objects.async_count() == 0
            assert await AsyncBook.objects.async_count() == 0  # CASCADE
            assert await AsyncArticle.objects.async_count() == 3  # NULLIFY
            assert await AsyncBlog.objects.async_count() == 1  # PULL
            
            # Check nullified articles
            for article in articles:
                reloaded = await AsyncArticle.objects.async_get(id=article.id)
                assert reloaded.author is None
            
            # Check blog authors list
            blog_reloaded = await AsyncBlog.objects.async_get(id=blog.id)
            assert len(blog_reloaded.authors) == 0
            
        finally:
            # Cleanup
            await AsyncAuthor.async_drop_collection()
            await AsyncBook.async_drop_collection()
            await AsyncArticle.async_drop_collection()
            await AsyncBlog.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_bulk_delete_with_cascade(self):
        """Test bulk delete operations with cascade rules."""
        # Define documents for this test
        class AsyncAuthor(Document):
            name = StringField(required=True)
            meta = {"collection": "test_bulk_authors", "db_alias": "async_cascade_test"}
        
        class AsyncBook(Document):
            title = StringField(required=True)
            author = ReferenceField(AsyncAuthor, reverse_delete_rule=CASCADE)
            meta = {"collection": "test_bulk_books", "db_alias": "async_cascade_test"}
        
        try:
            # Create multiple authors
            authors = []
            for i in range(3):
                author = AsyncAuthor(name=f"Bulk Author {i}")
                await author.async_save()
                authors.append(author)
            
            # Create books for each author
            for author in authors:
                for j in range(2):
                    book = AsyncBook(title=f"Book by {author.name} #{j}", author=author)
                    await book.async_save()
            
            # Verify setup
            assert await AsyncAuthor.objects.async_count() == 3
            assert await AsyncBook.objects.async_count() == 6
            
            # Bulk delete authors
            await AsyncAuthor.objects.filter(name__startswith="Bulk").async_delete()
            
            # Verify cascade deletion
            assert await AsyncAuthor.objects.async_count() == 0
            assert await AsyncBook.objects.async_count() == 0
            
        finally:
            # Cleanup
            await AsyncAuthor.async_drop_collection()
            await AsyncBook.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_self_reference_cascade(self):
        """Test cascade delete with self-referencing documents."""
        # Create a document type with self-reference
        class AsyncNode(Document):
            name = StringField(required=True)
            parent = ReferenceField('self', reverse_delete_rule=CASCADE)
            meta = {"collection": "test_self_ref_nodes", "db_alias": "async_cascade_test"}
        
        try:
            # Create hierarchy
            root = AsyncNode(name="root")
            await root.async_save()
            
            child1 = AsyncNode(name="child1", parent=root)
            child2 = AsyncNode(name="child2", parent=root)
            await child1.async_save()
            await child2.async_save()
            
            grandchild = AsyncNode(name="grandchild", parent=child1)
            await grandchild.async_save()
            
            # Delete root - should cascade to all descendants
            await root.async_delete()
            
            # Verify all nodes are deleted
            assert await AsyncNode.objects.async_count() == 0
            
        finally:
            # Cleanup
            await AsyncNode.async_drop_collection()