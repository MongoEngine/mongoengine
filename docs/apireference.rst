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
   :inherited-members:

   .. attribute:: objects

      A :class:`~mongoengine.queryset.QuerySet` object that is created lazily
      on access.

.. autoclass:: mongoengine.EmbeddedDocument
   :members:
   :inherited-members:

.. autoclass:: mongoengine.DynamicDocument
   :members:
   :inherited-members:

.. autoclass:: mongoengine.DynamicEmbeddedDocument
   :members:
   :inherited-members:

.. autoclass:: mongoengine.document.MapReduceDocument
   :members:

.. autoclass:: mongoengine.ValidationError
  :members:

.. autoclass:: mongoengine.FieldDoesNotExist


Context Managers
================

.. autoclass:: mongoengine.context_managers.switch_db
.. autoclass:: mongoengine.context_managers.switch_collection
.. autoclass:: mongoengine.context_managers.no_dereference
.. autoclass:: mongoengine.context_managers.query_counter

Querying
========

.. automodule:: mongoengine.queryset
    :synopsis: Queryset level operations

    .. autoclass:: mongoengine.queryset.QuerySet
      :members:
      :inherited-members:

      .. automethod:: QuerySet.__call__

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
.. autoclass:: mongoengine.fields.EmbeddedDocumentListField
.. autoclass:: mongoengine.fields.SortedListField
.. autoclass:: mongoengine.fields.DictField
.. autoclass:: mongoengine.fields.MapField
.. autoclass:: mongoengine.fields.ReferenceField
.. autoclass:: mongoengine.fields.LazyReferenceField
.. autoclass:: mongoengine.fields.GenericReferenceField
.. autoclass:: mongoengine.fields.GenericLazyReferenceField
.. autoclass:: mongoengine.fields.CachedReferenceField
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
.. autoclass:: mongoengine.fields.MultiPointField
.. autoclass:: mongoengine.fields.MultiLineStringField
.. autoclass:: mongoengine.fields.MultiPolygonField
.. autoclass:: mongoengine.fields.GridFSError
.. autoclass:: mongoengine.fields.GridFSProxy
.. autoclass:: mongoengine.fields.ImageGridFsProxy
.. autoclass:: mongoengine.fields.ImproperlyConfigured

Embedded Document Querying
==========================

.. versionadded:: 0.9

Additional queries for Embedded Documents are available when using the
:class:`~mongoengine.EmbeddedDocumentListField` to store a list of embedded
documents.

A list of embedded documents is returned as a special list with the
following methods:

.. autoclass:: mongoengine.base.datastructures.EmbeddedDocumentList
    :members:

Misc
====

.. autofunction:: mongoengine.common._import_class
