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

.. autoclass:: mongoengine.queryset.QuerySetNoCache
   :members:

   .. automethod:: mongoengine.queryset.QuerySetNoCache.__call__

.. autofunction:: mongoengine.queryset.queryset_manager

Fields
======

.. autoclass:: mongoengine.base.fields.BaseField
.. autoclass:: mongoengine.fields.StringField
.. autoclass:: mongoengine.fields.URLField
.. autoclass:: mongoengine.fields.EmailField
.. autoclass:: mongoengine.fields.IntField
.. autoclass:: mongoengine.fields.LongField
.. autoclass:: mongoengine.fields.FloatField
.. autoclass:: mongoengine.fields.DecimalField
.. autoclass:: mongoengine.fields.BooleanField
.. autoclass:: mongoengine.fields.DateTimeField
.. autoclass:: mongoengine.fields.ComplexDateTimeField
.. autoclass:: mongoengine.fields.EmbeddedDocumentField
.. autoclass:: mongoengine.fields.GenericEmbeddedDocumentField
.. autoclass:: mongoengine.fields.DynamicField
.. autoclass:: mongoengine.fields.ListField
.. autoclass:: mongoengine.fields.SortedListField
.. autoclass:: mongoengine.fields.DictField
.. autoclass:: mongoengine.fields.MapField
.. autoclass:: mongoengine.fields.ReferenceField
.. autoclass:: mongoengine.fields.GenericReferenceField
.. autoclass:: mongoengine.fields.BinaryField
.. autoclass:: mongoengine.fields.FileField
.. autoclass:: mongoengine.fields.ImageField
.. autoclass:: mongoengine.fields.SequenceField
.. autoclass:: mongoengine.fields.ObjectIdField
.. autoclass:: mongoengine.fields.UUIDField
.. autoclass:: mongoengine.fields.GeoPointField
.. autoclass:: mongoengine.fields.PointField
.. autoclass:: mongoengine.fields.LineStringField
.. autoclass:: mongoengine.fields.PolygonField
.. autoclass:: mongoengine.fields.GridFSError
.. autoclass:: mongoengine.fields.GridFSProxy
.. autoclass:: mongoengine.fields.ImageGridFsProxy
.. autoclass:: mongoengine.fields.ImproperlyConfigured

Misc
====

.. autofunction:: mongoengine.common._import_class
