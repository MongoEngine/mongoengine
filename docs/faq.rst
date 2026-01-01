==========================
Frequently Asked Questions
==========================

Does MongoEngine support asynchronous operations?
-------------------------------------------------

Yes, as of version 0.3.0, MongoEngine provides built-in support for asynchronous operations.
This support is based on PyMongo's native asynchronous driver (available in PyMongo 4.14+).

You can use the :attr:`~mongoengine.Document.aobjects` attribute for asynchronous queries and
methods like :meth:`~mongoengine.Document.asave` and :meth:`~mongoengine.Document.adelete` for
document operations.

For more details, see the :doc:`guide/querying` and :doc:`guide/connecting` sections of the documentation.

Does MongoEngine support other asynchronous drivers (Motor, TxMongo)?
---------------------------------------------------------------------

No, MongoEngine's asynchronous support is exclusively based on PyMongo's native async implementation
and isn't designed to support other drivers. If you specifically need to use Motor or TxMongo,
you might want to check out `uMongo`_.

.. _uMongo: https://umongo.readthedocs.io/en/latest/
