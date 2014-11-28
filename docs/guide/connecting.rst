.. _guide-connecting:

=====================
Connecting to MongoDB
=====================

To connect to a running instance of :program:`mongod`, use the
:func:`~mongoengine.connect` function. The first argument is the name of the
database to connect to::

    from mongoengine import connect
    connect('project1')

By default, MongoEngine assumes that the :program:`mongod` instance is running
on **localhost** on port **27017**. If MongoDB is running elsewhere, you should
provide the :attr:`host` and :attr:`port` arguments to
:func:`~mongoengine.connect`::

    connect('project1', host='192.168.1.35', port=12345)

If the database requires authentication, :attr:`username` and :attr:`password`
arguments should be provided::

    connect('project1', username='webapp', password='pwd123')

URI style connections are also supported -- just supply the URI as
the :attr:`host` to
:func:`~mongoengine.connect`::

    connect('project1', host='mongodb://localhost/database_name')

.. note:: Database, username and password from URI string overrides
    corresponding parameters in :func:`~mongoengine.connect`: ::

        connect(
            name='test',
            username='user',
            password='12345',
            host='mongodb://admin:qwerty@localhost/production'
        )

    will establish connection to ``production`` database using
    ``admin`` username and ``qwerty`` password.

ReplicaSets
===========

MongoEngine supports
:class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`. To use them,
please use an URI style connection and provide the ``replicaSet`` name
in the connection kwargs.

Read preferences are supported through the connection or via individual
queries by passing the read_preference ::

    Bar.objects().read_preference(ReadPreference.PRIMARY)
    Bar.objects(read_preference=ReadPreference.PRIMARY)

Multiple Databases
==================

Multiple database support was added in MongoEngine 0.6. To use multiple
databases you can use :func:`~mongoengine.connect` and provide an `alias` name
for the connection - if no `alias` is provided then "default" is used.

In the background this uses :func:`~mongoengine.register_connection` to
store the data and you can register all aliases up front if required.

Individual documents can also support multiple databases by providing a
`db_alias` in their meta data.  This allows :class:`~pymongo.dbref.DBRef` objects
to point across databases and collections.  Below is an example schema, using
3 different databases to store data::

        class User(Document):
            name = StringField()

            meta = {"db_alias": "user-db"}

        class Book(Document):
            name = StringField()

            meta = {"db_alias": "book-db"}

        class AuthorBooks(Document):
            author = ReferenceField(User)
            book = ReferenceField(Book)

            meta = {"db_alias": "users-books-db"}


Context Managers
================
Sometimes you may want to switch the database or collection to query against
for a class.
For example, archiving older data into a separate database for performance
reasons or writing functions that dynamically choose collections to write
document to.

Switch Database
---------------
The :class:`~mongoengine.context_managers.switch_db` context manager allows
you to change the database alias for a given class allowing quick and easy
access the same User document across databases::

    from mongoengine.context_managers import switch_db

    class User(Document):
        name = StringField()

        meta = {"db_alias": "user-db"}

    with switch_db(User, 'archive-user-db') as User:
        User(name="Ross").save()  # Saves the 'archive-user-db'


Switch Collection
-----------------
The :class:`~mongoengine.context_managers.switch_collection` context manager
allows you to change the collection for a given class allowing quick and easy
access the same Group document across collection::

        from mongoengine.context_managers import switch_collection

        class Group(Document):
            name = StringField()

        Group(name="test").save()  # Saves in the default db

        with switch_collection(Group, 'group2000') as Group:
            Group(name="hello Group 2000 collection!").save()  # Saves in group2000 collection



.. note:: Make sure any aliases have been registered with
    :func:`~mongoengine.register_connection` or :func:`~mongoengine.connect`
    before using the context manager.
