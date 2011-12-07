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
