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

Querying
========

.. autoclass:: mongoengine.queryset.QuerySet
   :members:

   .. automethod:: mongoengine.queryset.QuerySet.__call__

.. autofunction:: mongoengine.queryset.queryset_manager

Fields
======

.. autoclass:: mongoengine.BinaryField
.. autoclass:: mongoengine.BooleanField
.. autoclass:: mongoengine.ComplexDateTimeField
.. autoclass:: mongoengine.DateTimeField
.. autoclass:: mongoengine.DecimalField
.. autoclass:: mongoengine.DictField
.. autoclass:: mongoengine.DynamicField
.. autoclass:: mongoengine.EmailField
.. autoclass:: mongoengine.EmbeddedDocumentField
.. autoclass:: mongoengine.FileField
.. autoclass:: mongoengine.FloatField
.. autoclass:: mongoengine.GenericEmbeddedDocumentField
.. autoclass:: mongoengine.GenericReferenceField
.. autoclass:: mongoengine.GeoPointField
.. autoclass:: mongoengine.ImageField
.. autoclass:: mongoengine.IntField
.. autoclass:: mongoengine.ListField
.. autoclass:: mongoengine.MapField
.. autoclass:: mongoengine.ObjectIdField
.. autoclass:: mongoengine.ReferenceField
.. autoclass:: mongoengine.SequenceField
.. autoclass:: mongoengine.SortedListField
.. autoclass:: mongoengine.StringField
.. autoclass:: mongoengine.URLField
.. autoclass:: mongoengine.UUIDField
