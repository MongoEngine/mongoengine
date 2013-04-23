=============
API Reference
=============

Connecting
==========

.. autofunction:: mongoengine.connect
.. autofunction:: mongoengine.register_connection

Documents
=========

.. autoclass:: mongoengine.Document
   :members:

   .. attribute:: objects

      A :class:`~mongoengine.queryset.QuerySet` object that is created lazily
      on access.

.. autoclass:: mongoengine.EmbeddedDocument
   :members:

.. autoclass:: mongoengine.DynamicDocument
   :members:

.. autoclass:: mongoengine.DynamicEmbeddedDocument
   :members:

.. autoclass:: mongoengine.document.MapReduceDocument
  :members:

.. autoclass:: mongoengine.ValidationError
  :members:

Context Managers
================

.. autoclass:: mongoengine.context_managers.switch_db
.. autoclass:: mongoengine.context_managers.no_dereference
.. autoclass:: mongoengine.context_managers.query_counter

Querying
========

.. autoclass:: mongoengine.queryset.QuerySet
   :members:

   .. automethod:: mongoengine.queryset.QuerySet.__call__

.. autofunction:: mongoengine.queryset.queryset_manager

Fields
======

.. autoclass:: mongoengine.StringField
.. autoclass:: mongoengine.URLField
.. autoclass:: mongoengine.EmailField
.. autoclass:: mongoengine.IntField
.. autoclass:: mongoengine.LongField
.. autoclass:: mongoengine.FloatField
.. autoclass:: mongoengine.DecimalField
.. autoclass:: mongoengine.BooleanField
.. autoclass:: mongoengine.DateTimeField
.. autoclass:: mongoengine.ComplexDateTimeField
.. autoclass:: mongoengine.EmbeddedDocumentField
.. autoclass:: mongoengine.GenericEmbeddedDocumentField
.. autoclass:: mongoengine.DynamicField
.. autoclass:: mongoengine.ListField
.. autoclass:: mongoengine.SortedListField
.. autoclass:: mongoengine.DictField
.. autoclass:: mongoengine.MapField
.. autoclass:: mongoengine.ReferenceField
.. autoclass:: mongoengine.GenericReferenceField
.. autoclass:: mongoengine.BinaryField
.. autoclass:: mongoengine.FileField
.. autoclass:: mongoengine.ImageField
.. autoclass:: mongoengine.GeoPointField
.. autoclass:: mongoengine.SequenceField
.. autoclass:: mongoengine.ObjectIdField
.. autoclass:: mongoengine.UUIDField
.. autoclass:: mongoengine.GridFSError
.. autoclass:: mongoengine.GridFSProxy
.. autoclass:: mongoengine.ImageGridFsProxy
.. autoclass:: mongoengine.ImproperlyConfigured
