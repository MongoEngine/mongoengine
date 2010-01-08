==========
User Guide
==========

Installing
==========
MongoEngine is available on PyPI, so to use it you can use 
:program:`easy_install`
    
.. code-block:: console

    # easy_install mongoengine

Alternatively, if you don't have setuptools installed, `download it from PyPi 
<http://pypi.python.org/pypi/mongoengine/>`_ and run

.. code-block:: console

    # python setup.py install

.. _guide-connecting:

Connecting to MongoDB
=====================
To connect to a running instance of :program:`mongod`, use the
:func:`~mongoengine.connect` function. The first argument is the name of the
database to connect to. If the database does not exist, it will be created. If
the database requires authentication, :attr:`username` and :attr:`password`
arguments may be provided::

    from mongoengine import connect
    connect('project1', username='webapp', password='pwd123')

By default, MongoEngine assumes that the :program:`mongod` instance is running
on **localhost** on port **27017**. If MongoDB is running elsewhere, you may
provide :attr:`host` and :attr:`port` arguments to
:func:`~mongoengine.connect`::

    connect('project1', host='192.168.1.35', port=12345)

Defining documents
==================
In MongoDB, a **document** is roughly equivalent to a **row** in an RDBMS. When
working with relational databases, rows are stored in **tables**, which have a
strict **schema** that the rows follow. MongoDB stores documents in
**collections** rather than tables - the principle difference is that no schema 
is enforced at a database level. 

Defining a document's schema
----------------------------
MongoEngine allows you to define schemata for documents as this helps to reduce
coding errors, and allows for utility methods to be defined on fields which may
be present. 

To define a schema for a document, create a class that inherits from
:class:`~mongoengine.Document`. Fields are specified by adding **field
objects** as class attributes to the document class::

    from mongoengine import *
    import datetime
    
    class Page(Document):
        title = StringField(max_length=200, required=True)
        date_modified = DateTimeField(default=datetime.now)

Fields
------
By default, fields are not required. To make a field mandatory, set the
:attr:`required` keyword argument of a field to ``True``. Fields also may have
validation constraints available (such as :attr:`max_length` in the example
above). Fields may also take default values, which will be used if a value is
not provided. Default values may optionally be a callable, which will be called
to retrieve the value (such as in the above example). The field types available 
are as follows:

* :class:`~mongoengine.StringField`
* :class:`~mongoengine.IntField`
* :class:`~mongoengine.FloatField`
* :class:`~mongoengine.DateTimeField`
* :class:`~mongoengine.ListField`
* :class:`~mongoengine.ObjectIdField`
* :class:`~mongoengine.EmbeddedDocumentField`
* :class:`~mongoengine.ReferenceField`

List fields
^^^^^^^^^^^
MongoDB allows the storage of lists of items. To add a list of items to a
:class:`~mongoengine.Document`, use the :class:`~mongoengine.ListField` field
type. :class:`~mongoengine.ListField` takes another field object as its first
argument, which specifies which type elements may be stored within the list::

    class Page(Document):
        tags = ListField(StringField(max_length=50))

Embedded documents
^^^^^^^^^^^^^^^^^^
MongoDB has the ability to embed documents within other documents. Schemata may
be defined for these embedded documents, just as they may be for regular
documents. To create an embedded document, just define a document as usual, but
inherit from :class:`~mongoengine.EmbeddedDocument` rather than 
:class:`~mongoengine.Document`::

    class Comment(EmbeddedDocument):
        content = StringField()

To embed the document within another document, use the
:class:`~mongoengine.EmbeddedDocumentField` field type, providing the embedded
document class as the first argument::

    class Page(Document):
        comments = ListField(EmbeddedDocumentField(Comment))

    comment1 = Comment('Good work!')
    comment2 = Comment('Nice article!')
    page = Page(comments=[comment1, comment2])

Reference fields
^^^^^^^^^^^^^^^^
References may be stored to other documents in the database using the
:class:`~mongoengine.ReferenceField`. Pass in another document class as the
first argument to the constructor, then simply assign document objects to the
field::
    
    class User(Document):
        name = StringField()

    class Page(Document):
        content = StringField()
        author = ReferenceField(User)

    john = User(name="John Smith")
    john.save()

    post = Page(content="Test Page")
    post.author = john
    post.save()

The :class:`User` object is automatically turned into a reference behind the
scenes, and dereferenced when the :class:`Page` object is retrieved.

Uniqueness constraints
^^^^^^^^^^^^^^^^^^^^^^
MongoEngine allows you to specify that a field should be unique across a
collection by providing ``unique=True`` to a :class:`~mongoengine.Field`\ 's
constructor. If you try to save a document that has the same value for a unique
field as a document that is already in the database, a 
:class:`~mongoengine.OperationError` will be raised. You may also specify
multi-field uniqueness constraints by using :attr:`unique_with`, which may be
either a single field name, or a list or tuple of field names::

    class User(Document):
        username = StringField(unique=True)
        first_name = StringField()
        last_name = StringField(unique_with='last_name')

Document collections
--------------------
Document classes that inherit **directly** from :class:`~mongoengine.Document`
will have their own **collection** in the database. The name of the collection
is by default the name of the class, coverted to lowercase (so in the example
above, the collection would be called `page`). If you need to change the name
of the collection (e.g. to use MongoEngine with an existing database), then
create a class dictionary attribute called :attr:`meta` on your document, and
set :attr:`collection` to the name of the collection that you want your
document class to use::

    class Page(Document):
        title = StringField(max_length=200, required=True)
        meta = {'collection': 'cmsPage'}

Capped collections
^^^^^^^^^^^^^^^^^^
A :class:`~mongoengine.Document` may use a **Capped Collection** by specifying
:attr:`max_documents` and :attr:`max_size` in the :attr:`meta` dictionary.
:attr:`max_documents` is the maximum number of documents that is allowed to be
stored in the collection, and :attr:`max_size` is the maximum size of the
collection in bytes. If :attr:`max_size` is not specified and
:attr:`max_documents` is, :attr:`max_size` defaults to 10000000 bytes (10MB).
The following example shows a :class:`Log` document that will be limited to 
1000 entries and 2MB of disk space::

    class Log(Document):
        ip_address = StringField()
        meta = {'max_documents': 1000, 'max_size': 2000000}

Indexes
-------
You can specify indexes on collections to make querying faster. This is done
by creating a list of index specifications called :attr:`indexes` in the
:attr:`~mongoengine.Document.meta` dictionary, where an index specification may
either be a single field name, or a tuple containing multiple field names. A
direction may be specified on fields by prefixing the field name with a **+**
or a **-** sign. Note that direction only matters on multi-field indexes. ::

    class Page(Document):
        title = StringField()
        rating = StringField()
        meta = {
            'indexes': ['title', ('title', '-rating')]
        }
        
Ordering
--------
A default ordering can be specified for your
:class:`~mongoengine.queryset.QuerySet` using the :attr:`ordering` attribute of
:attr:`~mongoengine.Document.meta`.  Ordering will be applied when the
:class:`~mongoengine.queryset.QuerySet` is created, and can be overridden by
subsequent calls to :meth:`~mongoengine.queryset.QuerySet.order_by`. ::

    from datetime import datetime

    class BlogPost(Document):
        title = StringField()
        published_date = DateTimeField()

        meta = {
            'ordering': ['-published_date']
        }

    blog_post_1 = BlogPost(title="Blog Post #1")
    blog_post_1.published_date = datetime(2010, 1, 5, 0, 0 ,0))

    blog_post_2 = BlogPost(title="Blog Post #2") 
    blog_post_2.published_date = datetime(2010, 1, 6, 0, 0 ,0))

    blog_post_3 = BlogPost(title="Blog Post #3")
    blog_post_3.published_date = datetime(2010, 1, 7, 0, 0 ,0))

    blog_post_1.save()
    blog_post_2.save()
    blog_post_3.save()

    # get the "first" BlogPost using default ordering
    # from BlogPost.meta.ordering
    latest_post = BlogPost.objects.first() 
    self.assertEqual(latest_post.title, "Blog Post #3")

    # override default ordering, order BlogPosts by "published_date"
    first_post = BlogPost.objects.order_by("+published_date").first()
    self.assertEqual(first_post.title, "Blog Post #1")

Document inheritance
--------------------
To create a specialised type of a :class:`~mongoengine.Document` you have
defined, you may subclass it and add any extra fields or methods you may need.
As this is new class is not a direct subclass of
:class:`~mongoengine.Document`, it will not be stored in its own collection; it
will use the same collection as its superclass uses. This allows for more
convenient and efficient retrieval of related documents::

    # Stored in a collection named 'page'
    class Page(Document):
        title = StringField(max_length=200, required=True)

    # Also stored in the collection named 'page'
    class DatedPage(Page):
        date = DateTimeField()

Working with existing data
^^^^^^^^^^^^^^^^^^^^^^^^^^
To enable correct retrieval of documents involved in this kind of heirarchy,
two extra attributes are stored on each document in the database: :attr:`_cls`
and :attr:`_types`. These are hidden from the user through the MongoEngine
interface, but may not be present if you are trying to use MongoEngine with 
an existing database. For this reason, you may disable this inheritance
mechansim, removing the dependency of :attr:`_cls` and :attr:`_types`, enabling
you to work with existing databases. To disable inheritance on a document
class, set :attr:`allow_inheritance` to ``False`` in the :attr:`meta`
dictionary::

    # Will work with data in an existing collection named 'cmsPage'
    class Page(Document):
        title = StringField(max_length=200, required=True)
        meta = {
            'collection': 'cmsPage',
            'allow_inheritance': False,
        }

Documents instances
===================
To create a new document object, create an instance of the relevant document
class, providing values for its fields as its constructor keyword arguments.
You may provide values for any of the fields on the document::
    
    >>> page = Page(title="Test Page")
    >>> page.title
    'Test Page'

You may also assign values to the document's fields using standard object 
attribute syntax::

    >>> page.title = "Example Page"
    >>> page.title
    'Example Page'

Saving and deleting documents
-----------------------------
To save the document to the database, call the
:meth:`~mongoengine.Document.save` method. If the document does not exist in
the database, it will be created. If it does already exist, it will be
updated.

To delete a document, call the :meth:`~mongoengine.Document.delete` method.
Note that this will only work if the document exists in the database and has a
valide :attr:`id`.

Document IDs
------------
Each document in the database has a unique id. This may be accessed through the
:attr:`id` attribute on :class:`~mongoengine.Document` objects. Usually, the id
will be generated automatically by the database server when the object is save,
meaning that you may only access the :attr:`id` field once a document has been
saved::

    >>> page = Page(title="Test Page")
    >>> page.id
    >>> page.save()
    >>> page.id
    ObjectId('123456789abcdef000000000')

Alternatively, you may explicitly set the :attr:`id` before you save the
document, but the id must be a valid PyMongo :class:`ObjectId`.

Querying the database
=====================
:class:`~mongoengine.Document` classes have an :attr:`objects` attribute, which
is used for accessing the objects in the database associated with the class.
The :attr:`objects` attribute is actually a
:class:`~mongoengine.queryset.QuerySetManager`, which creates and returns a new
a new :class:`~mongoengine.queryset.QuerySet` object on access. The
:class:`~mongoengine.queryset.QuerySet` object may may be iterated over to
fetch documents from the database::

    # Prints out the names of all the users in the database
    for user in User.objects:
        print user.name

Filtering queries
-----------------
The query may be filtered by calling the
:class:`~mongoengine.queryset.QuerySet` object with field lookup keyword 
arguments. The keys in the keyword arguments correspond to fields on the
:class:`~mongoengine.Document` you are querying::

    # This will return a QuerySet that will only iterate over users whose
    # 'country' field is set to 'uk'
    uk_users = User.objects(country='uk')

Fields on embedded documents may also be referred to using field lookup syntax
by using a double-underscore in place of the dot in object attribute access
syntax::
    
    # This will return a QuerySet that will only iterate over pages that have
    # been written by a user whose 'country' field is set to 'uk'
    uk_pages = Page.objects(author__country='uk')

Querying lists
^^^^^^^^^^^^^^
On most fields, this syntax will look up documents where the field specified
matches the given value exactly, but when the field refers to a
:class:`~mongoengine.ListField`, a single item may be provided, in which case
lists that contain that item will be matched::

    class Page(Document):
        tags = ListField(StringField())

    # This will match all pages that have the word 'coding' as an item in the
    # 'tags' list
    Page.objects(tags='coding')

Query operators
---------------
Operators other than equality may also be used in queries; just attach the
operator name to a key with a double-underscore::
    
    # Only find users whose age is 18 or less
    young_users = Users.objects(age__lte=18)

Available operators are as follows:

* ``neq`` -- not equal to
* ``lt`` -- less than
* ``lte`` -- less than or equal to
* ``gt`` -- greater than
* ``gte`` -- greater than or equal to
* ``in`` -- value is in list (a list of values should be provided)
* ``nin`` -- value is not in list (a list of values should be provided)
* ``mod`` -- ``value % x == y``, where ``x`` and ``y`` are two provided values
* ``all`` -- every item in array is in list of values provided
* ``size`` -- the size of the array is 
* ``exists`` -- value for field exists

Limiting and skipping results
-----------------------------
Just as with traditional ORMs, you may limit the number of results returned, or
skip a number or results in you query.
:meth:`~mongoengine.queryset.QuerySet.limit` and
:meth:`~mongoengine.queryset.QuerySet.skip` and methods are available on
:class:`~mongoengine.queryset.QuerySet` objects, but the prefered syntax for
achieving this is using array-slicing syntax::

    # Only the first 5 people
    users = User.objects[:5]

    # All except for the first 5 people
    users = User.objects[5:]

    # 5 users, starting from the 10th user found
    users = User.objects[10:15]

Aggregation
-----------
MongoDB provides some aggregation methods out of the box, but there are not as
many as you typically get with an RDBMS. MongoEngine provides a wrapper around
the built-in methods and provides some of its own, which are implemented as
Javascript code that is executed on the database server.

Counting results
^^^^^^^^^^^^^^^^
Just as with limiting and skipping results, there is a method on
:class:`~mongoengine.queryset.QuerySet` objects -- 
:meth:`~mongoengine.queryset.QuerySet.count`, but there is also a more Pythonic
way of achieving this::

    num_users = len(User.objects)

Further aggregation
^^^^^^^^^^^^^^^^^^^
You may sum over the values of a specific field on documents using
:meth:`~mongoengine.queryset.QuerySet.sum`::

    yearly_expense = Employee.objects.sum('salary')

.. note::
   If the field isn't present on a document, that document will be ignored from
   the sum.

To get the average (mean) of a field on a collection of documents, use
:meth:`~mongoengine.queryset.QuerySet.average`::

    mean_age = User.objects.average('age')

As MongoDB provides native lists, MongoEngine provides a helper method to get a
dictionary of the frequencies of items in lists across an entire collection --
:meth:`~mongoengine.queryset.QuerySet.item_frequencies`. An example of its use
would be generating "tag-clouds"::

    class Article(Document):
        tag = ListField(StringField())

    # After adding some tagged articles...
    tag_freqs = Article.objects.item_frequencies('tag', normalize=True)

    from operator import itemgetter
    top_tags = sorted(tag_freqs.items(), key=itemgetter(1), reverse=True)[:10]

Atomic updates
--------------
Documents may be updated atomically by using the
:meth:`~mongoengine.queryset.QuerySet.update_one` and
:meth:`~mongoengine.queryset.QuerySet.update` methods on a 
:meth:`~mongoengine.queryset.QuerySet`. There are several different "modifiers"
that you may use with these methods:

* ``set`` -- set a particular value
* ``unset`` -- delete a particular value (since MongoDB v1.3+)
* ``inc`` -- increment a value by a given amount
* ``dec`` -- decrement a value by a given amount
* ``push`` -- append a value to a list
* ``push_all`` -- append several values to a list
* ``pull`` -- remove a value from a list
* ``pull_all`` -- remove several values from a list

The syntax for atomic updates is similar to the querying syntax, but the 
modifier comes before the field, not after it::
    
    >>> post = BlogPost(title='Test', page_views=0, tags=['database'])
    >>> post.save()
    >>> BlogPost.objects(id=post.id).update_one(inc__page_views=1)
    >>> post.reload()  # the document has been changed, so we need to reload it
    >>> post.page_views
    1
    >>> BlogPost.objects(id=post.id).update_one(set__title='Example Post')
    >>> post.reload()
    >>> post.title
    'Example Post'
    >>> BlogPost.objects(id=post.id).update_one(push__tags='nosql')
    >>> post.reload()
    >>> post.tags
    ['database', 'nosql']

