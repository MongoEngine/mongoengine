===============
Async Support
===============

MongoEngine provides comprehensive asynchronous support using PyMongo's AsyncMongoClient.
This allows you to build high-performance applications using async/await syntax while
maintaining full compatibility with existing synchronous code.

Getting Started
===============

To use async features, connect to MongoDB using :func:`~mongoengine.connect_async`
instead of the regular :func:`~mongoengine.connect`:

.. code-block:: python

    import asyncio
    from mongoengine import Document, StringField, connect_async

    # Connect asynchronously
    await connect_async('mydatabase')

    class User(Document):
        name = StringField(required=True)
        email = StringField(required=True)

All document models remain exactly the same - there's no need for separate async document classes.

Basic Operations
================

Document CRUD Operations
-------------------------

All document operations have async equivalents with the ``async_`` prefix:

.. code-block:: python

    # Create and save
    user = User(name="John", email="john@example.com")
    await user.async_save()

    # Reload from database
    await user.async_reload()

    # Delete
    await user.async_delete()

    # Ensure indexes
    await User.async_ensure_indexes()

    # Drop collection
    await User.async_drop_collection()

QuerySet Operations
===================

Basic Queries
-------------

.. code-block:: python

    # Get single document
    user = await User.objects.async_get(name="John")
    
    # Get first matching document
    user = await User.objects.filter(email__contains="@gmail.com").async_first()
    
    # Count documents
    count = await User.objects.async_count()
    total_users = await User.objects.filter(active=True).async_count()
    
    # Check existence
    exists = await User.objects.filter(name="John").async_exists()
    
    # Convert to list
    users = await User.objects.filter(active=True).async_to_list()

Async Iteration
---------------

Use ``async for`` to iterate over query results efficiently:

.. code-block:: python

    # Iterate over all users
    async for user in User.objects.filter(active=True):
        print(f"User: {user.name} ({user.email})")

    # Iterate with ordering and limits
    async for user in User.objects.order_by('-created_at').limit(10):
        await process_user(user)

Bulk Operations
===============

Create Operations
-----------------

.. code-block:: python

    # Create single document
    user = await User.objects.async_create(name="Jane", email="jane@example.com")

    # Bulk insert (not yet implemented - use regular save in loop)
    users = []
    for i in range(100):
        user = User(name=f"User{i}", email=f"user{i}@example.com")
        await user.async_save()
        users.append(user)

Update Operations
-----------------

.. code-block:: python

    # Update single document
    result = await User.objects.filter(name="John").async_update_one(email="newemail@example.com")
    
    # Bulk update
    result = await User.objects.filter(active=False).async_update(active=True)
    
    # Update with operators
    await User.objects.filter(name="John").async_update(inc__login_count=1)
    await User.objects.filter(email="old@example.com").async_update(
        set__email="new@example.com",
        inc__update_count=1
    )

Delete Operations
-----------------

.. code-block:: python

    # Delete matching documents
    result = await User.objects.filter(active=False).async_delete()
    
    # Delete with cascade (if references exist)
    await User.objects.filter(name="John").async_delete()

Reference Fields
================

Async Dereferencing
-------------------

In async context, reference fields return an ``AsyncReferenceProxy`` that requires explicit fetching:

.. code-block:: python

    class Post(Document):
        title = StringField(required=True)
        author = ReferenceField(User)

    class Comment(Document):
        text = StringField(required=True)
        post = ReferenceField(Post)
        author = ReferenceField(User)

    # Create and save documents
    user = User(name="Alice", email="alice@example.com")
    await user.async_save()
    
    post = Post(title="My Post", author=user)
    await post.async_save()

    # In async context, explicitly fetch references
    fetched_post = await Post.objects.async_first()
    author = await fetched_post.author.async_fetch()  # Returns User instance
    print(f"Post by: {author.name}")

Lazy Reference Fields
---------------------

.. code-block:: python

    from mongoengine import LazyReferenceField

    class Post(Document):
        title = StringField(required=True)
        author = LazyReferenceField(User)

    post = await Post.objects.async_first()
    # Async fetch for lazy references
    author = await post.author.async_fetch()

GridFS Operations
=================

File Storage
------------

.. code-block:: python

    from mongoengine import Document, FileField
    
    class MyDocument(Document):
        name = StringField()
        file = FileField()

    # Store file asynchronously
    doc = MyDocument(name="My Document")
    
    with open("example.txt", "rb") as f:
        file_data = f.read()
    
    # Put file
    await MyDocument.file.async_put(file_data, instance=doc, filename="example.txt")
    await doc.async_save()

    # Read file
    file_content = await MyDocument.file.async_read(doc)
    
    # Get file metadata
    file_proxy = await MyDocument.file.async_get(doc)
    print(f"File size: {file_proxy.length}")
    
    # Delete file
    await MyDocument.file.async_delete(doc)
    
    # Replace file
    await MyDocument.file.async_replace(new_file_data, doc, filename="new_example.txt")

Transactions
============

MongoDB transactions are supported through the ``async_run_in_transaction`` context manager:

.. code-block:: python

    from mongoengine import async_run_in_transaction

    async def transfer_funds():
        async with async_run_in_transaction():
            # All operations within this block are transactional
            sender = await Account.objects.async_get(user_id="sender123")
            receiver = await Account.objects.async_get(user_id="receiver456")
            
            sender.balance -= 100
            receiver.balance += 100
            
            await sender.async_save()
            await receiver.async_save()
            
            # Automatically commits on success, rolls back on exception

    # Usage
    try:
        await transfer_funds()
        print("Transfer completed successfully")
    except Exception as e:
        print(f"Transfer failed: {e}")

.. note::
    Transactions require MongoDB to be running as a replica set or sharded cluster.

Context Managers
================

Database Switching
------------------

.. code-block:: python

    from mongoengine import async_switch_db

    # Temporarily use different database
    async with async_switch_db(User, 'analytics_db') as UserAnalytics:
        analytics_user = UserAnalytics(name="Analytics User")
        await analytics_user.async_save()

Collection Switching
--------------------

.. code-block:: python

    from mongoengine import async_switch_collection

    # Temporarily use different collection
    async with async_switch_collection(User, 'archived_users') as ArchivedUser:
        archived = ArchivedUser(name="Archived User")
        await archived.async_save()

Disable Dereferencing
---------------------

.. code-block:: python

    from mongoengine import async_no_dereference

    # Disable automatic reference dereferencing for performance
    async with async_no_dereference(Post) as PostNoDereference:
        posts = await PostNoDereference.objects.async_to_list()
        # author field contains ObjectId instead of User instance

Aggregation Framework
=====================

Aggregation Pipelines
----------------------

.. code-block:: python

    # Basic aggregation
    pipeline = [
        {"$match": {"active": True}},
        {"$group": {
            "_id": "$department", 
            "count": {"$sum": 1},
            "avg_salary": {"$avg": "$salary"}
        }},
        {"$sort": {"count": -1}}
    ]

    results = []
    cursor = await User.objects.async_aggregate(pipeline)
    async for doc in cursor:
        results.append(doc)

    print(f"Found {len(results)} departments")

Distinct Values
---------------

.. code-block:: python

    # Get unique values
    departments = await User.objects.async_distinct("department")
    active_emails = await User.objects.filter(active=True).async_distinct("email")
    
    # Distinct on embedded documents
    cities = await User.objects.async_distinct("address.city")

Advanced Features
=================

Cascade Operations
------------------

All cascade delete rules work with async operations:

.. code-block:: python

    class Author(Document):
        name = StringField(required=True)

    class Book(Document):
        title = StringField(required=True)
        author = ReferenceField(Author, reverse_delete_rule=CASCADE)

    # When author is deleted, all books are automatically deleted
    author = await Author.objects.async_get(name="John Doe")
    await author.async_delete()  # This cascades to delete all books

Mixed Sync/Async Usage
======================

You can use both sync and async operations in the same application by using different connections:

.. code-block:: python

    # Sync connection
    connect('mydb', alias='sync_conn')

    # Async connection  
    await connect_async('mydb', alias='async_conn')

    # Configure models to use specific connections
    class SyncUser(Document):
        name = StringField()
        meta = {'db_alias': 'sync_conn'}

    class AsyncUser(Document):
        name = StringField()
        meta = {'db_alias': 'async_conn'}

    # Use appropriate methods for each connection type
    sync_user = SyncUser(name="Sync User")
    sync_user.save()  # Regular save

    async_user = AsyncUser(name="Async User")
    await async_user.async_save()  # Async save

Error Handling
==============

Connection Type Errors
-----------------------

MongoEngine enforces correct usage of sync/async methods:

.. code-block:: python

    # This will raise RuntimeError
    try:
        user = User(name="Test")
        user.save()  # Wrong! Using sync method with async connection
    except RuntimeError as e:
        print(f"Error: {e}")  # "Use async_save() with async connection"

Common Async Patterns
======================

Batch Processing
----------------

.. code-block:: python

    async def process_users_in_batches(batch_size=100):
        total = await User.objects.async_count()
        processed = 0
        
        while processed < total:
            batch = await User.objects.skip(processed).limit(batch_size).async_to_list()
            
            for user in batch:
                await process_single_user(user)
                await user.async_save()
            
            processed += len(batch)
            print(f"Processed {processed}/{total} users")

Error Recovery
--------------

.. code-block:: python

    async def save_with_retry(document, max_retries=3):
        for attempt in range(max_retries):
            try:
                await document.async_save()
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                print(f"Save failed (attempt {attempt + 1}), retrying: {e}")
                await asyncio.sleep(1)

Performance Tips
================

1. **Use async iteration**: ``async for`` is more memory efficient than ``async_to_list()``
2. **Batch operations**: Use bulk update/delete when possible
3. **Explicit reference fetching**: Only fetch references when needed
4. **Connection pooling**: PyMongo handles this automatically for async connections
5. **Avoid mixing**: Don't mix sync and async operations in the same connection

Current Limitations
===================

The following features are intentionally not implemented:

async_values() and async_values_list()
---------------------------------------

Field projection methods are not yet implemented due to low usage frequency.

**Workaround**: Use aggregation pipeline with ``$project``:

.. code-block:: python

    # Instead of: names = await User.objects.async_values_list('name', flat=True)
    # Use aggregation:
    pipeline = [{"$project": {"name": 1, "_id": 0}}]
    cursor = await User.objects.async_aggregate(pipeline)
    names = [doc['name'] async for doc in cursor]

async_explain()
---------------

Query execution plan analysis is not implemented as it's primarily a debugging feature.

**Workaround**: Use PyMongo directly:

.. code-block:: python

    from mongoengine.connection import get_db
    
    db = get_db()  # Get async database
    collection = db[User._get_collection_name()]
    explanation = await collection.find({"active": True}).explain()

Hybrid Signal System
--------------------

Automatic sync/async signal handling is not implemented due to complexity.
Current signals work only with synchronous operations.

ListField with ReferenceField
------------------------------

Automatic AsyncReferenceProxy conversion for references inside ListField is not supported.

**Workaround**: Manual dereferencing:

.. code-block:: python

    class Post(Document):
        authors = ListField(ReferenceField(User))

    post = await Post.objects.async_first()
    # Manual dereferencing required
    authors = []
    for author_ref in post.authors:
        if hasattr(author_ref, 'fetch'):  # Check if it's a LazyReference
            author = await author_ref.fetch()
        else:
            # It's an ObjectId, fetch manually
            author = await User.objects.async_get(id=author_ref)
        authors.append(author)

Migration from Sync to Async
=============================

Step-by-Step Migration
----------------------

1. **Update connection**:
   
   .. code-block:: python

       # Before
       connect('mydb')
       
       # After  
       await connect_async('mydb')

2. **Update function signatures**:

   .. code-block:: python

       # Before
       def get_user(name):
           return User.objects.get(name=name)
       
       # After
       async def get_user(name):
           return await User.objects.async_get(name=name)

3. **Update method calls**:

   .. code-block:: python

       # Before
       user.save()
       users = User.objects.filter(active=True)
       for user in users:
           process(user)
       
       # After
       await user.async_save()
       async for user in User.objects.filter(active=True):
           await process(user)

4. **Update reference access**:

   .. code-block:: python

       # Before (sync context)
       author = post.author  # Automatic dereferencing
       
       # After (async context)
       author = await post.author.async_fetch()  # Explicit fetching

Compatibility Notes
===================

- **100% Backward Compatibility**: Existing sync code works unchanged when using ``connect()``
- **No Model Changes**: Document models require no modifications
- **Clear Error Messages**: Wrong method usage provides helpful guidance
- **Performance**: Async operations provide better I/O concurrency
- **MongoDB Support**: Works with all MongoDB versions supported by MongoEngine

For more examples and advanced usage, see the `API Reference <../apireference.html>`_.