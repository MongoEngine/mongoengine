.. _guide-connecting:

=====================
Connecting to MongoDB
=====================

Connections in MongoEngine are registered globally and are identified with aliases.
If no ``alias`` is provided during the connection, it will use "default" as alias.

To connect to a running instance of :program:`mongod`, use the :func:`~mongoengine.connect`
function. The first argument is the name of the database to connect to::

    from mongoengine import connect
    connect('project1')

By default, MongoEngine assumes that the :program:`mongod` instance is running
on **localhost** on port **27017**.

If MongoDB is running elsewhere, you need to provide details on how to connect. There are two ways of
doing this. Using a connection string in URI format (**this is the preferred method**) or individual attributes
provided as keyword arguments.

Connect with URI string
=======================

When using a connection string in URI format you should specify the connection details
as the :attr:`host` to :func:`~mongoengine.connect`. In a web application context for instance, the URI
is typically read from the config file::

        connect(host="mongodb://127.0.0.1:27017/my_db")

If the database requires authentication, you can specify it in the
URI. As each database can have its own users configured, you need to tell MongoDB
where to look for the user you are working with, that's what the ``?authSource=admin`` bit
of the MongoDB connection string is for::

    # Connects to 'my_db' database by authenticating
    # with given credentials against the 'admin' database (by default as authSource isn't provided)
    connect(host="mongodb://my_user:my_password@127.0.0.1:27017/my_db")

    # Equivalent to previous connection but explicitly states that
    # it should use admin as the authentication source database
    connect(host="mongodb://my_user:my_password@hostname:port/my_db?authSource=admin")

    # Connects to 'my_db' database by authenticating
    # with given credentials against that same database
    connect(host="mongodb://my_user:my_password@127.0.0.1:27017/my_db?authSource=my_db")

The URI string can also be used to configure advanced parameters like ssl, replicaSet, etc. For more
information or example about URI string, you can refer to the `official doc <https://www.mongodb.com/docs/manual/reference/connection-string/>`_::

    connect(host="mongodb://my_user:my_password@127.0.0.1:27017/my_db?authSource=admin&ssl=true&replicaSet=globaldb")

.. note:: URI containing SRV records (e.g "mongodb+srv://server.example.com/") can be used as well

Connect with keyword attributes
===============================

The second option for specifying the connection details is to provide the information as keyword
attributes to :func:`~mongoengine.connect`::

    connect('my_db', host='127.0.0.1', port=27017)

If the database requires authentication, :attr:`username`, :attr:`password`
and :attr:`authentication_source` arguments should be provided::

    connect('my_db', username='my_user', password='my_password', authentication_source='admin')

The set of attributes that :func:`~mongoengine.connect` recognizes includes but is not limited to:
:attr:`host`, :attr:`port`, :attr:`read_preference`, :attr:`username`, :attr:`password`, :attr:`authentication_source`, :attr:`authentication_mechanism`,
:attr:`replicaset`, :attr:`tls`, etc. Most of the parameters accepted by `pymongo.MongoClient <https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient>`_
can be used with :func:`~mongoengine.connect` and will simply be forwarded when instantiating the `pymongo.MongoClient`.

.. note:: Database, username and password from URI string overrides
    corresponding parameters in :func:`~mongoengine.connect`, this should
    obviously be avoided: ::

        connect(
            db='test',
            username='user',
            password='12345',
            host='mongodb://admin:qwerty@localhost/production'
        )

    will establish connection to ``production`` database using ``admin`` username and ``qwerty`` password.

.. note:: Calling :func:`~mongoengine.connect` without argument will establish
    a connection to the "test" database by default

Read Preferences
================

As stated above, Read preferences are supported through the connection but also via individual
queries by passing the read_preference ::

    from pymongo import ReadPreference

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

.. note:: :func:`~mongoengine.context_managers.switch_db` when used on
    a class that allow inheritance will change the database alias
    for instances of a given class only - instances of subclasses will still use
    the default database.

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
