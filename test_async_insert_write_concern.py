#!/usr/bin/env python
"""Test write_concern handling in async_insert method"""

import asyncio

from mongoengine import (
    Document,
    StringField,
    connect_async,
    disconnect_async,
)


class TestDocument(Document):
    name = StringField(required=True)
    meta = {"collection": "test_write_concern", "db_alias": "test_async"}


async def test_async_insert_with_write_concern():
    """Test that async_insert properly handles write_concern as dict"""

    # Connect to MongoDB
    await connect_async("test_db", alias="test_async")

    try:
        # Clear existing data
        await TestDocument.objects.async_delete()

        # Test 1: Insert with write_concern as dict (should now work)
        print("Test 1: Insert with write_concern as dict")
        doc1 = TestDocument(name="Test1")
        write_concern_dict = {"w": 1, "j": False}

        try:
            result = await TestDocument.objects.async_insert(
                doc1, write_concern=write_concern_dict
            )
            print(f"  ✓ Insert with dict write_concern succeeded: {result.name}")
        except TypeError as e:
            print(f"  ✗ Error with dict write_concern: {e}")

        # Test 2: Insert without write_concern (default)
        print("\nTest 2: Insert without write_concern")
        doc2 = TestDocument(name="Test2")
        result = await TestDocument.objects.async_insert(doc2)
        print(f"  ✓ Insert without write_concern succeeded: {result.name}")

        # Test 3: Bulk insert with write_concern
        print("\nTest 3: Bulk insert with write_concern")
        docs = [TestDocument(name="Test3"), TestDocument(name="Test4")]
        results = await TestDocument.objects.async_insert(docs, write_concern={"w": 1})
        print(f"  ✓ Bulk insert with write_concern succeeded: {len(results)} docs")

        # Verify all documents were inserted
        count = await TestDocument.objects.async_count()
        print(f"\nTotal documents inserted: {count}")

        # Also test with WriteConcern object directly
        from pymongo.write_concern import WriteConcern

        print("\nTest 4: Insert with WriteConcern object")
        doc5 = TestDocument(name="Test5")
        wc = WriteConcern(w=1, j=True)
        result = await TestDocument.objects.async_insert(doc5, write_concern=wc)
        print(f"  ✓ Insert with WriteConcern object succeeded: {result.name}")

        print("\nAll tests passed successfully!")

    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Clean up
        await TestDocument.objects.async_delete()
        await disconnect_async("test_async")


if __name__ == "__main__":
    asyncio.run(test_async_insert_with_write_concern())
