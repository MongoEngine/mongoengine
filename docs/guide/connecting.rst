.. _guide-connecting:

=====================
Connecting to MongoDB
=====================

Connections in MongoEngine are registered globally and are identified with aliases.
If no `alias` is provided during the connection, it will use "default" as alias.

To connect to a running instance of :program:`mongod`, use the :func:`~mongoengine.connect`
function. The first argument is the name of the database to connect to::

    from mongoengine import connect
    connect('project1')

By default, MongoEngine assumes that the :program:`mongod` instance is running
on **localhost** on port **27017**. If MongoDB is running elsewhere, you should
provide the :attr:`host` and :attr:`port` arguments to
:func:`~mongoengine.connect`::

    connect('project1', host='192.168.1.35', port=12345)

If the database requires authentication, :attr:`username`, :attr:`password`
and :attr:`authentication_source` arguments should be provided::

    connect('project1', username='webapp', password='pwd123', authentication_source='admin')

URI style connections are also supported -- just supply the URI as
the :attr:`host` to
:func:`~mongoengine.connect`::

    connect('project1', host='mongodb://localhost/database_name')

.. note:: Database, username and password from URI string overrides
    corresponding parameters in :func:`~mongoengine.connect`: ::

        connect(
            db='test',
            username='user',
            password='12345',
            host='mongodb://admin:qwerty@localhost/production'
        )

    will establish connection to ``production`` database using
    ``admin`` username and ``qwerty`` password.

.. note:: Calling :func:`~mongoengine.connect` without argument will establish
    a connection to the "test" database by default

Replica Sets
============

MongoEngine supports connecting to replica sets::

    from mongoengine import connect

    # Regular connect
    connect('dbname', replicaset='rs-name')

    # MongoDB URI-style connect
    connect(host='mongodb://localhost/dbname?replicaSet=rs-name')

Read preferences are supported through the connection or via individual
queries by passing the read_preference ::

    Bar.objects().read_preference(ReadPreference.PRIMARY)
    Bar.objects(read_preference=ReadPreference.PRIMARY)

Multiple Databases
==================

To use multiple databases you can use :func:`~mongoengine.connect` and provide
an `alias` name for the connection - if no `alias` is provided then "default"
is used.

In the background this uses :func:`~mongoengine.register_connection` to
store the data and you can register all aliases up front if required.

Documents defined in different database
---------------------------------------
Individual documents can be attached to different databases by providing a
`db_alias` in their meta data. This allows :class:`~pymongo.dbref.DBRef`
objects to point across databases and collections. Below is an example schema,
using 3 different databases to store data::

        connect(alias='user-db-alias', db='user-db')
        connect(alias='book-db-alias', db='book-db')
        connect(alias='users-books-db-alias', db='users-books-db')
        
        class User(Document):
            name = StringField()

            meta = {'db_alias': 'user-db-alias'}

        class Book(Document):
            name = StringField()

            meta = {'db_alias': 'book-db-alias'}

        class AuthorBooks(Document):
            author = ReferenceField(User)
            book = ReferenceField(Book)

            meta = {'db_alias': 'users-books-db-alias'}


Disconnecting an existing connection
------------------------------------
The function :func:`~mongoengine.disconnect` can be used to
disconnect a particular connection. This can be used to change a
connection globally::

        from mongoengine import connect, disconnect
        connect('a_db', alias='db1')

        class User(Document):
            name = StringField()
            meta = {'db_alias': 'db1'}

        disconnect(alias='db1')

        connect('another_db', alias='db1')

.. note:: Calling :func:`~mongoengine.disconnect` without argument
    will disconnect the "default" connection

.. note:: Since connections gets registered globally, it is important
    to use the `disconnect` function from MongoEngine and not the
    `disconnect()` method of an existing connection (pymongo.MongoClient)

.. note:: :class:`~mongoengine.Document` are caching the pymongo collection.
    using `disconnect` ensures that it gets cleaned as well

Context Managers
================
Sometimes you may want to switch the database or collection to query against.
For example, archiving older data into a separate database for performance
reasons or writing functions that dynamically choose collections to write
a document to.

Switch Database
---------------
The :class:`~mongoengine.context_managers.switch_db` context manager allows
you to change the database alias for a given class allowing quick and easy
access to the same User document across databases::

    from mongoengine.context_managers import switch_db

    class User(Document):
        name = StringField()

        meta = {'db_alias': 'user-db'}

    with switch_db(User, 'archive-user-db') as User:
        User(name='Ross').save()  # Saves the 'archive-user-db'


Switch Collection
-----------------
The :func:`~mongoengine.context_managers.switch_collection` context manager
allows you to change the collection for a given class allowing quick and easy
access to the same Group document across collection::

        from mongoengine.context_managers import switch_collection

        class Group(Document):
            name = StringField()

        Group(name='test').save()  # Saves in the default db

        with switch_collection(Group, 'group2000') as Group:
            Group(name='hello Group 2000 collection!').save()  # Saves in group2000 collection


.. note:: Make sure any aliases have been registered with
    :func:`~mongoengine.register_connection` or :func:`~mongoengine.connect`
    before using the context manager.
