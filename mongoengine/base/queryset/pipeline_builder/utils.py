from __future__ import annotations

__all__ = ("needs_aggregation",)


def needs_aggregation(queryset):
    doc = queryset._document
    lf = queryset._loaded_fields
    projections = lf.as_dict() if lf else None

    from mongoengine.fields import (
        ReferenceField,
        EmbeddedDocumentField,
        EmbeddedDocumentListField,
        ListField,
        GenericReferenceField,
        DictField,
        MapField,
    )

    def is_list_of_embedded(fld):
        return (
                isinstance(fld, EmbeddedDocumentListField)
                or (
                        isinstance(fld, ListField)
                        and isinstance(getattr(fld, "field", None), EmbeddedDocumentField)
                )
        )

    def embedded_doc_type(fld):
        dt = getattr(fld, "document_type", None)
        if dt:
            return dt
        inner = getattr(fld, "field", None)
        dt = getattr(inner, "document_type", None) if inner else None
        if dt:
            return dt
        return None

    def unwrap_list(fld):
        cur = fld
        while isinstance(cur, ListField):
            cur = cur.field
        return cur

    def field_path_requires_lookup(parts):
        cls = doc
        for p in parts:
            if not cls:
                return False

            fld = cls._fields.get(p)
            if not fld:
                return False

            if isinstance(fld, (DictField, MapField)):
                sub = fld.field
                if isinstance(sub, ReferenceField):
                    return True
                if isinstance(sub, GenericReferenceField):
                    return bool(getattr(sub, "choices", None))
                if isinstance(sub, ListField):
                    leaf = unwrap_list(sub)
                    if isinstance(leaf, ReferenceField):
                        return True
                    if isinstance(leaf, GenericReferenceField):
                        return bool(getattr(leaf, "choices", None))

            if isinstance(fld, GenericReferenceField):
                return bool(getattr(fld, "choices", None))

            if isinstance(fld, ReferenceField):
                return True

            if isinstance(fld, ListField):
                leaf = unwrap_list(fld)
                if isinstance(leaf, ReferenceField):
                    return True
                if isinstance(leaf, GenericReferenceField):
                    return bool(getattr(leaf, "choices", None))

            if isinstance(fld, EmbeddedDocumentField) or is_list_of_embedded(fld):
                cls = embedded_doc_type(fld)
                continue

            cls = None

        return False

    mongo_query = queryset._query or {}
    for key in mongo_query.keys():
        if field_path_requires_lookup(key.split("__")):
            return True

    ordering = queryset._ordering or []
    for item in ordering:
        field = item[0] if isinstance(item, (tuple, list)) else item
        clean = field.lstrip("-").lstrip("+")
        if field_path_requires_lookup(clean.split("__")):
            return True

    def field_is_projected(name):
        if projections is None or projections == {}:
            return True
        if name in projections:
            return True
        return any(k.startswith(name + ".") for k in projections)

    def needs_lookup_for_field(field, seen_embedded=None):
        if seen_embedded is None:
            seen_embedded = set()

        if isinstance(field, (DictField, MapField)):
            sub = field.field
            if isinstance(sub, ReferenceField):
                return True
            if isinstance(sub, GenericReferenceField):
                return bool(getattr(sub, "choices", None))
            if isinstance(sub, ListField):
                leaf = unwrap_list(sub)
                if isinstance(leaf, ReferenceField):
                    return True
                if isinstance(leaf, GenericReferenceField):
                    return bool(getattr(leaf, "choices", None))

        if isinstance(field, GenericReferenceField):
            return bool(getattr(field, "choices", None))

        if isinstance(field, ReferenceField):
            return True

        if isinstance(field, ListField):
            leaf = unwrap_list(field)
            if isinstance(leaf, ReferenceField):
                return True
            if isinstance(leaf, GenericReferenceField):
                return bool(getattr(leaf, "choices", None))

        if isinstance(field, EmbeddedDocumentField) or is_list_of_embedded(field):
            dt = embedded_doc_type(field)
            if not dt or dt in seen_embedded:
                return False
            seen2 = set(seen_embedded)
            seen2.add(dt)
            return any(needs_lookup_for_field(sub, seen2) for sub in dt._fields.values())

        return False

    for name, field in doc._fields.items():
        if field_is_projected(name) and needs_lookup_for_field(field):
            return True

    return False
