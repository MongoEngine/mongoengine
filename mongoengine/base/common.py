from __future__ import absolute_import
from mongoengine.errors import NotRegistered, IndexCollisionError
import logging

logger = logging.getLogger(__name__)

__all__ = ("ALLOW_INHERITANCE", "get_document", "_document_registry")

ALLOW_INHERITANCE = False

_document_registry = {}


def get_document(name):
    doc = _document_registry.get(name, None)
    if not doc:
        # Possible old style name
        single_end = name.split(".")[-1]
        compound_end = ".%s" % single_end
        possible_match = [
            k
            for k in _document_registry.keys()
            if k.endswith(compound_end) or k == single_end
        ]
        if len(possible_match) == 1:
            doc = _document_registry.get(possible_match.pop(), None)
    if not doc:
        raise NotRegistered(
            """
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip()
            % name
        )
    return doc


class IndexRegistry:
    """Utilites to store and process index information"""

    def __init__(self, model_class):
        self.model_class = model_class
        self.index_specs = []  # stores full index specification of indexes
        self.index_fields = []  # stores the fields list of indexes

    def process_index(self, index_spec):
        return self.model_class._rippling_process_index_spec(index_spec)

    def add_index(self, index_spec):
        processed_index_spec = self.process_index(index_spec)
        if not processed_index_spec:
            return
        fields = processed_index_spec["fields"]
        if fields in self.index_fields:
            existing_index_spec = self.index_specs[self.index_fields.index(fields)]
            if index_spec.get("args", {}).get("override_existing_index", False):
                self.index_specs[self.index_fields.index(fields)] = processed_index_spec
            elif existing_index_spec != processed_index_spec:
                raise IndexCollisionError(
                    f"Index option collision on {fields} fields of {self.model_class}. Existing index {existing_index_spec} differs from required index {processed_index_spec}"
                )
        else:
            self.index_specs.append(processed_index_spec)
            self.index_fields.append(fields)

    def add_field_based_index(self, index_spec):
        """Add index where the index options are not essential"""
        processed_index_spec = self.process_index(index_spec)
        if not processed_index_spec:
            return
        fields = processed_index_spec["fields"]
        if fields in self.index_fields:
            # index with fields already present. Ditch the options
            return
        self.index_specs.append(processed_index_spec)
        self.index_fields.append(fields)

    def add_unique_index(self, index_spec):
        """Index for unique fields. Merges options if unique index on same field already exists"""
        processed_index_spec = self.process_index(index_spec)
        if not processed_index_spec:
            return
        fields = processed_index_spec["fields"]
        if fields in self.index_fields:
            existing_index_spec = self.index_specs[self.index_fields.index(fields)]
            if existing_index_spec == processed_index_spec:
                return  # index already exists
            options_mismatch = False
            if processed_index_spec.get("sparse", False) != existing_index_spec.get(
                "sparse", False
            ):
                # `sparse` mismatch
                options_mismatch = True
            if processed_index_spec.get("unique", False) != existing_index_spec.get(
                "unique", False
            ):
                # existing index is not unique
                options_mismatch = True
            if (
                "partialFilterExpression" in processed_index_spec
                and "partialFilterExpression" in existing_index_spec
                and processed_index_spec["partialFilterExpression"]
                != existing_index_spec["partialFilterExpression"]
            ):
                # exsiting index has different partial filter expression
                options_mismatch = True
            if options_mismatch:
                raise IndexCollisionError(
                    f"Error adding unique index on fields {fields} of {self.model_class}. Existing index {existing_index_spec} differs from required index {processed_index_spec}"
                )
            # merge the index if there's no option mismatch
            existing_index_spec.update(processed_index_spec)
        else:
            self.index_specs.append(processed_index_spec)
            self.index_fields.append(fields)
