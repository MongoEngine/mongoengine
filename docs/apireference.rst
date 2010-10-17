=============
API Reference
=============

Connecting
==========

.. autofunction:: mongoengine.connect

Documents
=========

.. autoclass:: mongoengine.Document
   :members:

   .. attribute:: objects

      A :class:`~mongoengine.queryset.QuerySet` object that is created lazily 
      on access.

.. autoclass:: mongoengine.EmbeddedDocument
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

.. autoclass:: mongoengine.IntField

.. autoclass:: mongoengine.FloatField

.. autoclass:: mongoengine.DecimalField

.. autoclass:: mongoengine.BooleanField

.. autoclass:: mongoengine.DateTimeField

.. autoclass:: mongoengine.EmbeddedDocumentField

.. autoclass:: mongoengine.DictField

.. autoclass:: mongoengine.ListField

.. autoclass:: mongoengine.BinaryField

.. autoclass:: mongoengine.ObjectIdField

.. autoclass:: mongoengine.ReferenceField

.. autoclass:: mongoengine.GenericReferenceField

.. autoclass:: mongoengine.FileField

.. autoclass:: mongoengine.GeoPointField
