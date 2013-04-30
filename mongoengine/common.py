_class_registry_cache = {}


def _import_class(cls_name):
    """Cached mechanism for imports"""
    if cls_name in _class_registry_cache:
        return _class_registry_cache.get(cls_name)

    doc_classes = ('Document', 'DynamicEmbeddedDocument', 'EmbeddedDocument',
                   'MapReduceDocument')
    field_classes = ('DictField', 'DynamicField', 'EmbeddedDocumentField',
                     'FileField', 'GenericReferenceField',
                     'GenericEmbeddedDocumentField', 'GeoPointField',
                     'PointField', 'LineStringField', 'PolygonField',
                     'ReferenceField', 'StringField', 'ComplexBaseField')
    queryset_classes = ('OperationError',)
    deref_classes = ('DeReference',)

    if cls_name in doc_classes:
        from mongoengine import document as module
        import_classes = doc_classes
    elif cls_name in field_classes:
        from mongoengine import fields as module
        import_classes = field_classes
    elif cls_name in queryset_classes:
        from mongoengine import queryset as module
        import_classes = queryset_classes
    elif cls_name in deref_classes:
        from mongoengine import dereference as module
        import_classes = deref_classes
    else:
        raise ValueError('No import set for: ' % cls_name)

    for cls in import_classes:
        _class_registry_cache[cls] = getattr(module, cls)

    return _class_registry_cache.get(cls_name)
