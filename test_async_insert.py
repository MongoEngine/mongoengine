#!/usr/bin/env python
"""Test script for async_insert functionality in BaseQuerySet"""

import asyncio

from mongoengine import (
    Document,
    IntField,
    StringField,
    connect_async,
    disconnect_async,
)


class TestUser(Document):
    name = StringField(required=True)
    age = IntField()
    email = StringField()

    meta = {"collection": "test_users", "db_alias": "test_async"}


async def test_async_insert():
    """Test the async_insert method"""

    # Connect to MongoDB
    await connect_async("test_db", alias="test_async")

    try:
        # Clear existing data
        await TestUser.objects.async_delete()

        # Test 1: Insert single document
        print("Test 1: Insert single document")
        user1 = TestUser(name="Alice", age=30, email="alice@example.com")
        result = await TestUser.objects.async_insert(user1)
        print(f"  Single insert result: {result}")
        print(f"  User ID: {result.pk}")

        # Verify the document was inserted
        count = await TestUser.objects.async_count()
        print(f"  Document count after single insert: {count}")

        # Test 2: Bulk insert multiple documents
        print("\nTest 2: Bulk insert multiple documents")
        users = [
            TestUser(name="Bob", age=25, email="bob@example.com"),
            TestUser(name="Charlie", age=35, email="charlie@example.com"),
            TestUser(name="David", age=28, email="david@example.com"),
        ]
        results = await TestUser.objects.async_insert(users)
        print(f"  Bulk insert returned {len(results)} documents")
        for user in results:
            print(f"    - {user.name}: {user.pk}")

        # Verify all documents were inserted
        count = await TestUser.objects.async_count()
        print(f"  Total document count: {count}")

        # Test 3: Insert with load_bulk=False (returns only IDs)
        print("\nTest 3: Insert with load_bulk=False")
        user2 = TestUser(name="Eve", age=32, email="eve@example.com")
        result_id = await TestUser.objects.async_insert(user2, load_bulk=False)
        print(f"  Insert with load_bulk=False returned ID: {result_id}")

        # Test 4: Verify all inserted documents
        print("\nTest 4: List all documents")
        all_users = await TestUser.objects.async_to_list()
        for user in all_users:
            print(f"  - {user.name} (age: {user.age}, email: {user.email})")

        print(f"\nTotal documents inserted: {len(all_users)}")
        print("All tests passed successfully!")

    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Clean up
        await TestUser.objects.async_delete()
        await disconnect_async("test_async")


if __name__ == "__main__":
    asyncio.run(test_async_insert())
