==================
Defining documents
==================
In MongoDB, a **document** is roughly equivalent to a **row** in an RDBMS. When
working with relational databases, rows are stored in **tables**, which have a
strict **schema** that the rows follow. MongoDB stores documents in
**collections** rather than tables - the principle difference is that no schema
is enforced at a database level.

Defining a document's schema
============================
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
        date_modified = DateTimeField(default=datetime.datetime.now)

As BSON (the binary format for storing data in mongodb) is order dependent,
documents are serialized based on their field order.

Dynamic document schemas
========================
One of the benefits of MongoDb is dynamic schemas for a collection, whilst data
should be planned and organised (after all explicit is better than implicit!)
there are scenarios where having dynamic / expando style documents is desirable.

:class:`~mongoengine.DynamicDocument` documents work in the same way as
:class:`~mongoengine.Document` but any data / attributes set to them will also
be saved ::

    from mongoengine import *

    class Page(DynamicDocument):
        title = StringField(max_length=200, required=True)

    # Create a new page and add tags
    >>> page = Page(title='Using MongoEngine')
    >>> page.tags = ['mongodb', 'mongoengine']
    >>> page.save()

    >>> Page.objects(tags='mongoengine').count()
    >>> 1

.. note::

   There is one caveat on Dynamic Documents: fields cannot start with `_`

Dynamic fields are stored in creation order *after* any declared fields.

Fields
======
By default, fields are not required. To make a field mandatory, set the
:attr:`required` keyword argument of a field to ``True``. Fields also may have
validation constraints available (such as :attr:`max_length` in the example
above). Fields may also take default values, which will be used if a value is
not provided. Default values may optionally be a callable, which will be called
to retrieve the value (such as in the above example). The field types available
are as follows:

* :class:`~mongoengine.fields.BinaryField`
* :class:`~mongoengine.fields.BooleanField`
* :class:`~mongoengine.fields.ComplexDateTimeField`
* :class:`~mongoengine.fields.DateTimeField`
* :class:`~mongoengine.fields.DecimalField`
* :class:`~mongoengine.fields.DictField`
* :class:`~mongoengine.fields.DynamicField`
* :class:`~mongoengine.fields.EmailField`
* :class:`~mongoengine.fields.EmbeddedDocumentField`
* :class:`~mongoengine.fields.FileField`
* :class:`~mongoengine.fields.FloatField`
* :class:`~mongoengine.fields.GenericEmbeddedDocumentField`
* :class:`~mongoengine.fields.GenericReferenceField`
* :class:`~mongoengine.fields.GeoPointField`
* :class:`~mongoengine.fields.ImageField`
* :class:`~mongoengine.fields.IntField`
* :class:`~mongoengine.fields.ListField`
* :class:`~mongoengine.fields.MapField`
* :class:`~mongoengine.fields.ObjectIdField`
* :class:`~mongoengine.fields.ReferenceField`
* :class:`~mongoengine.fields.SequenceField`
* :class:`~mongoengine.fields.SortedListField`
* :class:`~mongoengine.fields.StringField`
* :class:`~mongoengine.fields.URLField`
* :class:`~mongoengine.fields.UUIDField`

Field arguments
---------------
Each field type can be customized by keyword arguments.  The following keyword
arguments can be set on all fields:

:attr:`db_field` (Default: None)
    The MongoDB field name.

:attr:`required` (Default: False)
    If set to True and the field is not set on the document instance, a
    :class:`~mongoengine.ValidationError` will be raised when the document is
    validated.

:attr:`default` (Default: None)
    A value to use when no value is set for this field.

    The definion of default parameters follow `the general rules on Python
    <http://docs.python.org/reference/compound_stmts.html#function-definitions>`__,
    which means that some care should be taken when dealing with default mutable objects
    (like in :class:`~mongoengine.fields.ListField` or :class:`~mongoengine.fields.DictField`)::

        class ExampleFirst(Document):
            # Default an empty list
            values = ListField(IntField(), default=list)

        class ExampleSecond(Document):
            # Default a set of values
            values = ListField(IntField(), default=lambda: [1,2,3])

        class ExampleDangerous(Document):
            # This can make an .append call to  add values to the default (and all the following objects),
            # instead to just an object
            values = ListField(IntField(), default=[1,2,3])

    .. note:: Unsetting a field with a default value will revert back to the default.

:attr:`unique` (Default: False)
    When True, no documents in the collection will have the same value for this
    field.

:attr:`unique_with` (Default: None)
    A field name (or list of field names) that when taken together with this
    field, will not have two documents in the collection with the same value.

:attr:`primary_key` (Default: False)
    When True, use this field as a primary key for the collection.  `DictField`
    and `EmbeddedDocuments` both support being the primary key for a document.

:attr:`choices` (Default: None)
    An iterable (e.g. a list or tuple) of choices to which the value of this
    field should be limited.

    Can be either be a nested tuples of value (stored in mongo) and a
    human readable key ::

        SIZE = (('S', 'Small'),
                ('M', 'Medium'),
                ('L', 'Large'),
                ('XL', 'Extra Large'),
                ('XXL', 'Extra Extra Large'))


        class Shirt(Document):
            size = StringField(max_length=3, choices=SIZE)

    Or a flat iterable just containing values ::

        SIZE = ('S', 'M', 'L', 'XL', 'XXL')

        class Shirt(Document):
            size = StringField(max_length=3, choices=SIZE)

:attr:`help_text` (Default: None)
    Optional help text to output with the field - used by form libraries

:attr:`verbose_name` (Default: None)
    Optional human-readable name for the field - used by form libraries


List fields
-----------
MongoDB allows the storage of lists of items. To add a list of items to a
:class:`~mongoengine.Document`, use the :class:`~mongoengine.fields.ListField` field
type. :class:`~mongoengine.fields.ListField` takes another field object as its first
argument, which specifies which type elements may be stored within the list::

    class Page(Document):
        tags = ListField(StringField(max_length=50))

Embedded documents
------------------
MongoDB has the ability to embed documents within other documents. Schemata may
be defined for these embedded documents, just as they may be for regular
documents. To create an embedded document, just define a document as usual, but
inherit from :class:`~mongoengine.EmbeddedDocument` rather than
:class:`~mongoengine.Document`::

    class Comment(EmbeddedDocument):
        content = StringField()

To embed the document within another document, use the
:class:`~mongoengine.fields.EmbeddedDocumentField` field type, providing the embedded
document class as the first argument::

    class Page(Document):
        comments = ListField(EmbeddedDocumentField(Comment))

    comment1 = Comment(content='Good work!')
    comment2 = Comment(content='Nice article!')
    page = Page(comments=[comment1, comment2])

Dictionary Fields
-----------------
Often, an embedded document may be used instead of a dictionary -- generally
this is recommended as dictionaries don't support validation or custom field
types. However, sometimes you will not know the structure of what you want to
store; in this situation a :class:`~mongoengine.fields.DictField` is appropriate::

    class SurveyResponse(Document):
        date = DateTimeField()
        user = ReferenceField(User)
        answers = DictField()

    survey_response = SurveyResponse(date=datetime.now(), user=request.user)
    response_form = ResponseForm(request.POST)
    survey_response.answers = response_form.cleaned_data()
    survey_response.save()

Dictionaries can store complex data, other dictionaries, lists, references to
other objects, so are the most flexible field type available.

Reference fields
----------------
References may be stored to other documents in the database using the
:class:`~mongoengine.fields.ReferenceField`. Pass in another document class as the
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

To add a :class:`~mongoengine.fields.ReferenceField` that references the document
being defined, use the string ``'self'`` in place of the document class as the
argument to :class:`~mongoengine.fields.ReferenceField`'s constructor. To reference a
document that has not yet been defined, use the name of the undefined document
as the constructor's argument::

    class Employee(Document):
        name = StringField()
        boss = ReferenceField('self')
        profile_page = ReferenceField('ProfilePage')

    class ProfilePage(Document):
        content = StringField()


.. _one-to-many-with-listfields:

One to Many with ListFields
'''''''''''''''''''''''''''

If you are implementing a one to many relationship via a list of references,
then the references are stored as DBRefs and to query you need to pass an
instance of the object to the query::

    class User(Document):
        name = StringField()

    class Page(Document):
        content = StringField()
        authors = ListField(ReferenceField(User))

    bob = User(name="Bob Jones").save()
    john = User(name="John Smith").save()

    Page(content="Test Page", authors=[bob, john]).save()
    Page(content="Another Page", authors=[john]).save()

    # Find all pages Bob authored
    Page.objects(authors__in=[bob])

    # Find all pages that both Bob and John have authored
    Page.objects(authors__all=[bob, john])

    # Remove Bob from the authors for a page.
    Page.objects(id='...').update_one(pull__authors=bob)

    # Add John to the authors for a page.
    Page.objects(id='...').update_one(push__authors=john)


Dealing with deletion of referred documents
'''''''''''''''''''''''''''''''''''''''''''
By default, MongoDB doesn't check the integrity of your data, so deleting
documents that other documents still hold references to will lead to consistency
issues.  Mongoengine's :class:`ReferenceField` adds some functionality to
safeguard against these kinds of database integrity problems, providing each
reference with a delete rule specification.  A delete rule is specified by
supplying the :attr:`reverse_delete_rule` attributes on the
:class:`ReferenceField` definition, like this::

    class Employee(Document):
        ...
        profile_page = ReferenceField('ProfilePage', reverse_delete_rule=mongoengine.NULLIFY)

The declaration in this example means that when an :class:`Employee` object is
removed, the :class:`ProfilePage` that belongs to that employee is removed as
well.  If a whole batch of employees is removed, all profile pages that are
linked are removed as well.

Its value can take any of the following constants:

:const:`mongoengine.DO_NOTHING`
  This is the default and won't do anything.  Deletes are fast, but may cause
  database inconsistency or dangling references.
:const:`mongoengine.DENY`
  Deletion is denied if there still exist references to the object being
  deleted.
:const:`mongoengine.NULLIFY`
  Any object's fields still referring to the object being deleted are removed
  (using MongoDB's "unset" operation), effectively nullifying the relationship.
:const:`mongoengine.CASCADE`
  Any object containing fields that are refererring to the object being deleted
  are deleted first.
:const:`mongoengine.PULL`
  Removes the reference to the object (using MongoDB's "pull" operation)
  from any object's fields of
  :class:`~mongoengine.fields.ListField` (:class:`~mongoengine.fields.ReferenceField`).


.. warning::
   A safety note on setting up these delete rules!  Since the delete rules are
   not recorded on the database level by MongoDB itself, but instead at runtime,
   in-memory, by the MongoEngine module, it is of the upmost importance
   that the module that declares the relationship is loaded **BEFORE** the
   delete is invoked.

   If, for example, the :class:`Employee` object lives in the
   :mod:`payroll` app, and the :class:`ProfilePage` in the :mod:`people`
   app, it is extremely important that the :mod:`people` app is loaded
   before any employee is removed, because otherwise, MongoEngine could
   never know this relationship exists.

   In Django, be sure to put all apps that have such delete rule declarations in
   their :file:`models.py` in the :const:`INSTALLED_APPS` tuple.


.. warning::
   Signals are not triggered when doing cascading updates / deletes - if this
   is required you must manually handle the update / delete.

Generic reference fields
''''''''''''''''''''''''
A second kind of reference field also exists,
:class:`~mongoengine.fields.GenericReferenceField`. This allows you to reference any
kind of :class:`~mongoengine.Document`, and hence doesn't take a
:class:`~mongoengine.Document` subclass as a constructor argument::

    class Link(Document):
        url = StringField()

    class Post(Document):
        title = StringField()

    class Bookmark(Document):
        bookmark_object = GenericReferenceField()

    link = Link(url='http://hmarr.com/mongoengine/')
    link.save()

    post = Post(title='Using MongoEngine')
    post.save()

    Bookmark(bookmark_object=link).save()
    Bookmark(bookmark_object=post).save()

.. note::

   Using :class:`~mongoengine.fields.GenericReferenceField`\ s is slightly less
   efficient than the standard :class:`~mongoengine.fields.ReferenceField`\ s, so if
   you will only be referencing one document type, prefer the standard
   :class:`~mongoengine.fields.ReferenceField`.

Uniqueness constraints
----------------------
MongoEngine allows you to specify that a field should be unique across a
collection by providing ``unique=True`` to a :class:`~mongoengine.fields.Field`\ 's
constructor. If you try to save a document that has the same value for a unique
field as a document that is already in the database, a
:class:`~mongoengine.OperationError` will be raised. You may also specify
multi-field uniqueness constraints by using :attr:`unique_with`, which may be
either a single field name, or a list or tuple of field names::

    class User(Document):
        username = StringField(unique=True)
        first_name = StringField()
        last_name = StringField(unique_with='first_name')

Skipping Document validation on save
------------------------------------
You can also skip the whole document validation process by setting
``validate=False`` when calling the :meth:`~mongoengine.document.Document.save`
method::

    class Recipient(Document):
        name = StringField()
        email = EmailField()

    recipient = Recipient(name='admin', email='root@localhost')
    recipient.save()               # will raise a ValidationError while
    recipient.save(validate=False) # won't

Document collections
====================
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
------------------
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

.. defining-indexes_

Indexes
=======

You can specify indexes on collections to make querying faster. This is done
by creating a list of index specifications called :attr:`indexes` in the
:attr:`~mongoengine.Document.meta` dictionary, where an index specification may
either be a single field name, a tuple containing multiple field names, or a
dictionary containing a full index definition. A direction may be specified on
fields by prefixing the field name with a **+** (for ascending) or a **-** sign
(for descending). Note that direction only matters on multi-field indexes. ::

    class Page(Document):
        title = StringField()
        rating = StringField()
        meta = {
            'indexes': ['title', ('title', '-rating')]
        }

If a dictionary is passed then the following options are available:

:attr:`fields` (Default: None)
    The fields to index. Specified in the same format as described above.

:attr:`cls` (Default: True)
    If you have polymorphic models that inherit and have
    :attr:`allow_inheritance` turned on, you can configure whether the index
    should have the :attr:`_cls` field added automatically to the start of the
    index.

:attr:`sparse` (Default: False)
    Whether the index should be sparse.

:attr:`unique` (Default: False)
    Whether the index should be unique.

:attr:`expireAfterSeconds` (Optional)
    Allows you to automatically expire data from a collection by setting the
    time in seconds to expire the a field.

.. note::

    Inheritance adds extra fields indices see: :ref:`document-inheritance`.

Global index default options
----------------------------

There are a few top level defaults for all indexes that can be set::

    class Page(Document):
        title = StringField()
        rating = StringField()
        meta = {
            'index_options': {},
            'index_background': True,
            'index_drop_dups': True,
            'index_cls': False
        }


:attr:`index_options` (Optional)
    Set any default index options - see the `full options list <http://docs.mongodb.org/manual/reference/method/db.collection.ensureIndex/#db.collection.ensureIndex>`_

:attr:`index_background` (Optional)
    Set the default value for if an index should be indexed in the background

:attr:`index_drop_dups` (Optional)
    Set the default value for if an index should drop duplicates

:attr:`index_cls` (Optional)
    A way to turn off a specific index for _cls.


Compound Indexes and Indexing sub documents
-------------------------------------------

Compound indexes can be created by adding the Embedded field or dictionary
field name to the index definition.

Sometimes its more efficient to index parts of Embedded / dictionary fields,
in this case use 'dot' notation to identify the value to index eg: `rank.title`

Geospatial indexes
------------------

The best geo index for mongodb is the new "2dsphere", which has an improved
spherical model and provides better performance and more options when querying.
The following fields will explicitly add a "2dsphere" index:

    - :class:`~mongoengine.fields.PointField`
    - :class:`~mongoengine.fields.LineStringField`
    - :class:`~mongoengine.fields.PolygonField`

As "2dsphere" indexes can be part of a compound index, you may not want the
automatic index but would prefer a compound index.  In this example we turn off
auto indexing and explicitly declare a compound index on ``location`` and ``datetime``::

    class Log(Document):
        location = PointField(auto_index=False)
        datetime = DateTimeField()

        meta = {
            'indexes': [[("location", "2dsphere"), ("datetime", 1)]]
        }


Pre MongoDB 2.4 Geo
'''''''''''''''''''

.. note:: For MongoDB < 2.4 this is still current, however the new 2dsphere
    index is a big improvement over the previous 2D model - so upgrading is
    advised.

Geospatial indexes will be automatically created for all
:class:`~mongoengine.fields.GeoPointField`\ s

It is also possible to explicitly define geospatial indexes. This is
useful if you need to define a geospatial index on a subfield of a
:class:`~mongoengine.fields.DictField` or a custom field that contains a
point. To create a geospatial index you must prefix the field with the
***** sign. ::

    class Place(Document):
        location = DictField()
        meta = {
            'indexes': [
                '*location.point',
            ],
        }

Time To Live indexes
--------------------

A special index type that allows you to automatically expire data from a
collection after a given period. See the official
`ttl <http://docs.mongodb.org/manual/tutorial/expire-data/#expire-data-from-collections-by-setting-ttl>`_
documentation for more information.  A common usecase might be session data::

    class Session(Document):
        created = DateTimeField(default=datetime.now)
        meta = {
            'indexes': [
                {'fields': ['created'], 'expireAfterSeconds': 3600}
            ]
        }

.. warning:: TTL indexes happen on the MongoDB server and not in the application
    code, therefore no signals will be fired on document deletion.
    If you need signals to be fired on deletion, then you must handle the
    deletion of Documents in your application code.

Comparing Indexes
-----------------

Use :func:`mongoengine.Document.compare_indexes` to compare actual indexes in
the database to those that your document definitions define.  This is useful
for maintenance purposes and ensuring you have the correct indexes for your
schema.

Ordering
========
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
    blog_post_1.published_date = datetime(2010, 1, 5, 0, 0 ,0)

    blog_post_2 = BlogPost(title="Blog Post #2")
    blog_post_2.published_date = datetime(2010, 1, 6, 0, 0 ,0)

    blog_post_3 = BlogPost(title="Blog Post #3")
    blog_post_3.published_date = datetime(2010, 1, 7, 0, 0 ,0)

    blog_post_1.save()
    blog_post_2.save()
    blog_post_3.save()

    # get the "first" BlogPost using default ordering
    # from BlogPost.meta.ordering
    latest_post = BlogPost.objects.first()
    assert latest_post.title == "Blog Post #3"

    # override default ordering, order BlogPosts by "published_date"
    first_post = BlogPost.objects.order_by("+published_date").first()
    assert first_post.title == "Blog Post #1"

Shard keys
==========

If your collection is sharded, then you need to specify the shard key as a tuple,
using the :attr:`shard_key` attribute of :attr:`-mongoengine.Document.meta`.
This ensures that the shard key is sent with the query when calling the
:meth:`~mongoengine.document.Document.save` or
:meth:`~mongoengine.document.Document.update` method on an existing
:class:`-mongoengine.Document` instance::

    class LogEntry(Document):
        machine = StringField()
        app = StringField()
        timestamp = DateTimeField()
        data = StringField()

        meta = {
            'shard_key': ('machine', 'timestamp',)
        }

.. _document-inheritance:

Document inheritance
====================

To create a specialised type of a :class:`~mongoengine.Document` you have
defined, you may subclass it and add any extra fields or methods you may need.
As this is new class is not a direct subclass of
:class:`~mongoengine.Document`, it will not be stored in its own collection; it
will use the same collection as its superclass uses. This allows for more
convenient and efficient retrieval of related documents - all you need do is
set :attr:`allow_inheritance` to True in the :attr:`meta` data for a
document.::

    # Stored in a collection named 'page'
    class Page(Document):
        title = StringField(max_length=200, required=True)

        meta = {'allow_inheritance': True}

    # Also stored in the collection named 'page'
    class DatedPage(Page):
        date = DateTimeField()

.. note:: From 0.8 onwards you must declare :attr:`allow_inheritance` defaults
          to False, meaning you must set it to True to use inheritance.

Working with existing data
--------------------------
As MongoEngine no longer defaults to needing :attr:`_cls` you can quickly and
easily get working with existing data.  Just define the document to match
the expected schema in your database ::

    # Will work with data in an existing collection named 'cmsPage'
    class Page(Document):
        title = StringField(max_length=200, required=True)
        meta = {
            'collection': 'cmsPage'
        }

If you have wildly varying schemas then using a
:class:`~mongoengine.DynamicDocument` might be more appropriate, instead of
defining all possible field types.

If you use :class:`~mongoengine.Document` and the database contains data that
isn't defined then that data will be stored in the `document._data` dictionary.

Abstract classes
================

If you want to add some extra functionality to a group of Document classes but
you don't need or want the overhead of inheritance you can use the
:attr:`abstract` attribute of :attr:`-mongoengine.Document.meta`.
This won't turn on :ref:`document-inheritance` but will allow you to keep your
code DRY::

        class BaseDocument(Document):
            meta = {
                'abstract': True,
            }
            def check_permissions(self):
                ...

        class User(BaseDocument):
           ...

Now the User class will have access to the inherited `check_permissions` method
and won't store any of the extra `_cls` information.
