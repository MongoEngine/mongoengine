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
