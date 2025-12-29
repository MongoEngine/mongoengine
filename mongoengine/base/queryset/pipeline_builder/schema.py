from __future__ import annotations


class Schema:
    """
    Shared schema introspection helpers used by lookup_planner/match_planner/stage_builder.
    """

    @staticmethod
    def resolve_field_name(doc_cls, db_part: str):
        """Return (python_field_name, field_obj) by attr-name or db_field match."""
        if db_part in doc_cls._fields:
            return db_part, doc_cls._fields[db_part]
        for name, fld in doc_cls._fields.items():
            if getattr(fld, "db_field", None) == db_part:
                return name, fld
        return None, None

    @staticmethod
    def unwrap_list_leaf(field):
        """If the field is ListField(...ListField(x)...), return the deepest leaf."""
        from mongoengine.fields import ListField
        leaf = field
        while isinstance(leaf, ListField):
            leaf = leaf.field
        return leaf

    @staticmethod
    def unwrap_list_field(field):
        from mongoengine.fields import ListField
        if not isinstance(field, ListField):
            return None, 0
        depth = 0
        cur = field
        while isinstance(cur, ListField):
            depth += 1
            cur = cur.field
        return cur, depth

    @staticmethod
    def resolve_generic_choices(generic_field):
        """Return concrete document classes for a GenericReferenceField's choices."""
        from mongoengine.document import _DocumentRegistry

        out = []
        for ch in getattr(generic_field, "choices", None) or ():
            if isinstance(ch, str):
                cls = _DocumentRegistry.get(ch)
            elif isinstance(ch, type):
                cls = _DocumentRegistry.get(ch.__name__)
            else:
                cls = None
            if cls:
                out.append(cls)
        return out

    @staticmethod
    def cls_regex(cls) -> str:
        return f"^{cls._class_name}(\\.|$)"

    @staticmethod
    def regex_match(input_expr: str, cls) -> dict:
        return {"$regexMatch": {"input": input_expr, "regex": Schema.cls_regex(cls)}}
