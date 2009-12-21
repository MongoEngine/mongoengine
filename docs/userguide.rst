User Guide
==========

.. _guide-connecting:

Connecting to MongoDB
---------------------
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
------------------
In MongoDB, a **document** is roughly equivalent to a **row** in an RDBMS. When
working with relational databases, rows are stored in **tables**, which have a
strict **schema** that the rows follow. MongoDB stores documents in
**collections** rather than tables - the principle difference is that no schema 
is enforced at a database level. 

Defining a document's schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
^^^^^^
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
* :class:`~mongoengine.ObjectIdField`
* :class:`~mongoengine.EmbeddedDocumentField`
* :class:`~mongoengine.ReferenceField`

Document collections
^^^^^^^^^^^^^^^^^^^^
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
