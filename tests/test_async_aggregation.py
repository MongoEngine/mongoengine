"""Test async aggregation framework support."""

import pytest
import pytest_asyncio

from mongoengine import (
    Document,
    StringField,
    IntField,
    ListField,
    ReferenceField,
    EmbeddedDocument,
    EmbeddedDocumentField,
    connect_async,
    disconnect_async,
)


class TestAsyncAggregation:
    """Test async aggregation operations."""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Set up test database connections and clean up after."""
        await connect_async(db="mongoenginetest_async_agg", alias="default")
        
        yield
        
        # Cleanup
        await disconnect_async("default")
    
    @pytest.mark.asyncio
    async def test_async_aggregate_basic(self):
        """Test basic aggregation pipeline."""
        class Sale(Document):
            product = StringField(required=True)
            quantity = IntField(required=True)
            price = IntField(required=True)
            meta = {"collection": "test_agg_sales"}
        
        try:
            # Create test data
            await Sale(product="Widget", quantity=10, price=100).async_save()
            await Sale(product="Widget", quantity=5, price=100).async_save()
            await Sale(product="Gadget", quantity=3, price=200).async_save()
            await Sale(product="Gadget", quantity=7, price=200).async_save()
            
            # Aggregate by product
            pipeline = [
                {"$group": {
                    "_id": "$product",
                    "total_quantity": {"$sum": "$quantity"},
                    "total_revenue": {"$sum": {"$multiply": ["$quantity", "$price"]}},
                    "avg_quantity": {"$avg": "$quantity"}
                }},
                {"$sort": {"_id": 1}}
            ]
            
            results = []
            cursor = await Sale.objects.async_aggregate(pipeline)
            async for doc in cursor:
                results.append(doc)
            
            assert len(results) == 2
            
            # Check Gadget results
            gadget = next(r for r in results if r["_id"] == "Gadget")
            assert gadget["total_quantity"] == 10
            assert gadget["total_revenue"] == 2000
            assert gadget["avg_quantity"] == 5.0
            
            # Check Widget results
            widget = next(r for r in results if r["_id"] == "Widget")
            assert widget["total_quantity"] == 15
            assert widget["total_revenue"] == 1500
            assert widget["avg_quantity"] == 7.5
            
        finally:
            await Sale.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_aggregate_with_query(self):
        """Test aggregation with initial query filter."""
        class Order(Document):
            customer = StringField(required=True)
            status = StringField(choices=["pending", "completed", "cancelled"])
            amount = IntField(required=True)
            meta = {"collection": "test_agg_orders"}
        
        try:
            # Create test data
            await Order(customer="Alice", status="completed", amount=100).async_save()
            await Order(customer="Alice", status="completed", amount=200).async_save()
            await Order(customer="Alice", status="pending", amount=150).async_save()
            await Order(customer="Bob", status="completed", amount=300).async_save()
            await Order(customer="Bob", status="cancelled", amount=100).async_save()
            
            # Aggregate only completed orders
            pipeline = [
                {"$group": {
                    "_id": "$customer",
                    "total_completed": {"$sum": "$amount"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]
            
            results = []
            # Use queryset filter before aggregation
            cursor = await Order.objects.filter(status="completed").async_aggregate(pipeline)
            async for doc in cursor:
                results.append(doc)
            
            assert len(results) == 2
            
            alice = next(r for r in results if r["_id"] == "Alice")
            assert alice["total_completed"] == 300
            assert alice["count"] == 2
            
            bob = next(r for r in results if r["_id"] == "Bob")
            assert bob["total_completed"] == 300
            assert bob["count"] == 1
            
        finally:
            await Order.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_distinct_basic(self):
        """Test basic distinct operation."""
        class Product(Document):
            name = StringField(required=True)
            category = StringField(required=True)
            tags = ListField(StringField())
            meta = {"collection": "test_distinct_products"}
        
        try:
            # Create test data
            await Product(name="Widget A", category="widgets", tags=["new", "hot"]).async_save()
            await Product(name="Widget B", category="widgets", tags=["sale"]).async_save()
            await Product(name="Gadget A", category="gadgets", tags=["new"]).async_save()
            await Product(name="Gadget B", category="gadgets", tags=["hot", "sale"]).async_save()
            
            # Get distinct categories
            categories = await Product.objects.async_distinct("category")
            assert sorted(categories) == ["gadgets", "widgets"]
            
            # Get distinct tags
            tags = await Product.objects.async_distinct("tags")
            assert sorted(tags) == ["hot", "new", "sale"]
            
            # Get distinct categories with filter
            new_product_categories = await Product.objects.filter(tags="new").async_distinct("category")
            assert sorted(new_product_categories) == ["gadgets", "widgets"]
            
            # Get distinct tags for widgets only
            widget_tags = await Product.objects.filter(category="widgets").async_distinct("tags")
            assert sorted(widget_tags) == ["hot", "new", "sale"]
            
        finally:
            await Product.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_distinct_embedded(self):
        """Test distinct on embedded documents."""
        class Address(EmbeddedDocument):
            city = StringField(required=True)
            country = StringField(required=True)
        
        class Customer(Document):
            name = StringField(required=True)
            address = EmbeddedDocumentField(Address)
            meta = {"collection": "test_distinct_customers"}
        
        try:
            # Create test data
            await Customer(
                name="Alice",
                address=Address(city="New York", country="USA")
            ).async_save()
            
            await Customer(
                name="Bob",
                address=Address(city="London", country="UK")
            ).async_save()
            
            await Customer(
                name="Charlie",
                address=Address(city="New York", country="USA")
            ).async_save()
            
            await Customer(
                name="David",
                address=Address(city="Paris", country="France")
            ).async_save()
            
            # Get distinct cities
            cities = await Customer.objects.async_distinct("address.city")
            assert sorted(cities) == ["London", "New York", "Paris"]
            
            # Get distinct countries
            countries = await Customer.objects.async_distinct("address.country")
            assert sorted(countries) == ["France", "UK", "USA"]
            
            # Get distinct cities in USA
            usa_cities = await Customer.objects.filter(address__country="USA").async_distinct("address.city")
            assert usa_cities == ["New York"]
            
        finally:
            await Customer.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_aggregate_lookup(self):
        """Test aggregation with $lookup (join)."""
        class Author(Document):
            name = StringField(required=True)
            meta = {"collection": "test_agg_authors"}
        
        class Book(Document):
            title = StringField(required=True)
            author_id = ReferenceField(Author, dbref=False)  # Store as ObjectId
            year = IntField()
            meta = {"collection": "test_agg_books"}
        
        try:
            # Create test data
            author1 = Author(name="John Doe")
            await author1.async_save()
            
            author2 = Author(name="Jane Smith")
            await author2.async_save()
            
            await Book(title="Book 1", author_id=author1, year=2020).async_save()
            await Book(title="Book 2", author_id=author1, year=2021).async_save()
            await Book(title="Book 3", author_id=author2, year=2022).async_save()
            
            # Aggregate books with author info
            pipeline = [
                {"$lookup": {
                    "from": "test_agg_authors",
                    "localField": "author_id",
                    "foreignField": "_id",
                    "as": "author_info"
                }},
                {"$unwind": "$author_info"},
                {"$group": {
                    "_id": "$author_info.name",
                    "book_count": {"$sum": 1},
                    "latest_year": {"$max": "$year"}
                }},
                {"$sort": {"_id": 1}}
            ]
            
            results = []
            cursor = await Book.objects.async_aggregate(pipeline)
            async for doc in cursor:
                results.append(doc)
            
            assert len(results) == 2
            
            john = next(r for r in results if r["_id"] == "John Doe")
            assert john["book_count"] == 2
            assert john["latest_year"] == 2021
            
            jane = next(r for r in results if r["_id"] == "Jane Smith")
            assert jane["book_count"] == 1
            assert jane["latest_year"] == 2022
            
        finally:
            await Author.async_drop_collection()
            await Book.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_aggregate_with_sort_limit(self):
        """Test aggregation respects queryset sort and limit."""
        class Score(Document):
            player = StringField(required=True)
            score = IntField(required=True)
            meta = {"collection": "test_agg_scores"}
        
        try:
            # Create test data
            for i in range(10):
                await Score(player=f"Player{i}", score=i * 10).async_save()
            
            # Aggregate top 5 scores
            pipeline = [
                {"$project": {
                    "player": 1,
                    "score": 1,
                    "doubled": {"$multiply": ["$score", 2]}
                }}
            ]
            
            results = []
            # Apply sort and limit before aggregation
            cursor = await Score.objects.order_by("-score").limit(5).async_aggregate(pipeline)
            async for doc in cursor:
                results.append(doc)
            
            assert len(results) == 5
            
            # Should have the top 5 scores
            scores = [r["score"] for r in results]
            assert sorted(scores, reverse=True) == [90, 80, 70, 60, 50]
            
            # Check doubled values
            for result in results:
                assert result["doubled"] == result["score"] * 2
            
        finally:
            await Score.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_aggregate_empty_result(self):
        """Test aggregation with empty results."""
        class Event(Document):
            name = StringField(required=True)
            type = StringField(required=True)
            meta = {"collection": "test_agg_events"}
        
        try:
            # Create test data
            await Event(name="Event1", type="conference").async_save()
            await Event(name="Event2", type="workshop").async_save()
            
            # Aggregate with no matching documents
            pipeline = [
                {"$match": {"type": "webinar"}},  # No webinars exist
                {"$group": {
                    "_id": "$type",
                    "count": {"$sum": 1}
                }}
            ]
            
            results = []
            cursor = await Event.objects.async_aggregate(pipeline)
            async for doc in cursor:
                results.append(doc)
            
            assert len(results) == 0
            
        finally:
            await Event.async_drop_collection()
    
    @pytest.mark.asyncio
    async def test_async_distinct_no_results(self):
        """Test distinct with no matching documents."""
        class Item(Document):
            name = StringField(required=True)
            color = StringField()
            meta = {"collection": "test_distinct_items"}
        
        try:
            # Create test data
            await Item(name="Item1", color="red").async_save()
            await Item(name="Item2", color="blue").async_save()
            
            # Get distinct colors for non-existent items
            colors = await Item.objects.filter(name="NonExistent").async_distinct("color")
            assert colors == []
            
            # Get distinct values for field with no values
            await Item(name="Item3").async_save()  # No color
            all_names = await Item.objects.async_distinct("name")
            assert sorted(all_names) == ["Item1", "Item2", "Item3"]
            
        finally:
            await Item.async_drop_collection()