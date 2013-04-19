.. _guide-connecting:

=====================
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

Uri style connections are also supported as long as you include the database
name - just supply the uri as the :attr:`host` to
:func:`~mongoengine.connect`::

    connect('project1', host='mongodb://localhost/database_name')

ReplicaSets
===========

MongoEngine now supports :func:`~pymongo.replica_set_connection.ReplicaSetConnection`
to use them please use a URI style connection and provide the `replicaSet` name in the
connection kwargs.

Read preferences are supported throught the connection or via individual
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


Switch Database Context Manager
===============================

Sometimes you might want to switch the database to query against for a class.
The :class:`~mongoengine.context_managers.switch_db` context manager allows
you to change the database alias for a class eg ::

        from mongoengine.context_managers import switch_db

        class User(Document):
            name = StringField()

            meta = {"db_alias": "user-db"}

        with switch_db(User, 'archive-user-db') as User:
            User(name="Ross").save()  # Saves the 'archive-user-db'
