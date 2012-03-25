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
.. autoclass:: mongoengine.FloatField
.. autoclass:: mongoengine.DecimalField
.. autoclass:: mongoengine.DateTimeField
.. autoclass:: mongoengine.ComplexDateTimeField
.. autoclass:: mongoengine.ListField
.. autoclass:: mongoengine.SortedListField
.. autoclass:: mongoengine.DictField
.. autoclass:: mongoengine.MapField
.. autoclass:: mongoengine.ObjectIdField
.. autoclass:: mongoengine.ReferenceField
.. autoclass:: mongoengine.GenericReferenceField
.. autoclass:: mongoengine.EmbeddedDocumentField
.. autoclass:: mongoengine.GenericEmbeddedDocumentField
.. autoclass:: mongoengine.BooleanField
.. autoclass:: mongoengine.FileField
.. autoclass:: mongoengine.BinaryField
.. autoclass:: mongoengine.GeoPointField
.. autoclass:: mongoengine.SequenceField
