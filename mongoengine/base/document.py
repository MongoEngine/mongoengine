import operator
from functools import partial

import pymongo
from bson import json_util
from bson.son import SON

from mongoengine.common import _import_class
from mongoengine.errors import (ValidationError, LookUpError)
from mongoengine.python_support import PY3, txt_type

from mongoengine.base.proxy import DocumentProxy
from mongoengine.base.common import get_document, ALLOW_INHERITANCE
from mongoengine.base.datastructures import BaseDict, BaseList
from mongoengine.base.fields import ComplexBaseField
import collections

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'

_set = object.__setattr__


class BaseDocument(object):

    #_dynamic = False
    #_dynamic_lock = True
    _initialised = False

    def __init__(self, _son=None, **values):
        """
        Initialise a document or embedded document

        :param values: A dictionary of values for the document
        """
        _set(self, '_db_data', _son)
        _set(self, '_lazy', False)
        _set(self, '_internal_data', {})
        _set(self, '_changed_fields', set())
        if values:
            pk = values.pop('pk', None)
            for field in set(self._fields.keys()).intersection(list(values.keys())):
                setattr(self, field, values[field])
            if pk != None:
                self.pk = pk

    def __delattr__(self, name):
        default = self._fields[name].default
        value = default() if isinstance(default, collections.Callable) else default
        setattr(self, name, value)

    @property
    def _created(self):
        return self._db_data != None or self._lazy

    def __iter__(self):
        if 'id' in self._fields and 'id' not in self._fields_ordered:
            return iter(('id', ) + self._fields_ordered)

        return iter(self._fields_ordered)

    def __getitem__(self, name):
        """Dictionary-style field access, return a field's value if present.
        """
        try:
            if name in self._fields:
                return getattr(self, name)
        except AttributeError:
            pass
        raise KeyError(name)

    def __setitem__(self, name, value):
        """Dictionary-style field access, set a field's value.
        """
        # Ensure that the field exists before settings its value
        if name not in self._fields:
            raise KeyError(name)
        return setattr(self, name, value)

    def __contains__(self, name):
        try:
            val = getattr(self, name)
            return val is not None
        except AttributeError:
            return False

    def __unicode__(self):
        return '%s object' % self.__class__.__name__

    def __repr__(self):
        try:
            u = self.__str__()
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = '[Bad Unicode data]'
        repr_type = type(u)
        return repr_type('<%s: %s>' % (self.__class__.__name__, u))

    def __str__(self):
        if hasattr(self, '__unicode__'):
            if PY3:
                return self.__unicode__()
            else:
                return str(self).encode('utf-8')
        return txt_type('%s object' % self.__class__.__name__)

    def __eq__(self, other):
        if isinstance(other, DocumentProxy) and other._get_collection_name() == self._get_collection_name() and hasattr(other, 'pk') and self.pk == other.pk:
            return True

        if isinstance(other, self.__class__) and hasattr(other, 'pk') and self.pk == other.pk:
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if getattr(self, 'pk', None) is None:
            # For new object
            return super(BaseDocument, self).__hash__()
        else:
            return hash(self.pk)

    def clean(self):
        """
        Hook for doing document level data cleaning before validation is run.

        Any ValidationError raised by this method will not be associated with
        a particular field; it will have a special-case association with the
        field defined by NON_FIELD_ERRORS.
        """
        pass

    def to_mongo(self):
        """Return as SON data ready for use with MongoDB.
        """
        sets, unsets = self._delta(full=True)
        son = SON(data=sets)
        allow_inheritance = self._meta.get('allow_inheritance',
                                          ALLOW_INHERITANCE)
        if allow_inheritance:
            son['_cls'] = self._class_name
        return son

    def to_dict(self):
        return dict((field, getattr(self, field)) for field in self._fields)

    def validate(self, clean=True):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Ensure that each field is matched to a valid value
        errors = {}
        if clean:
            try:
                self.clean()
            except ValidationError as error:
                errors[NON_FIELD_ERRORS] = error

        # Get a list of tuples of field names and their current values
        fields = [(field, getattr(self, name))
                  for name, field in list(self._fields.items())]
        #if self._dynamic:
        #    fields += [(field, self._data.get(name))
        #               for name, field in self._dynamic_fields.items()]

        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        GenericEmbeddedDocumentField = _import_class("GenericEmbeddedDocumentField")

        for field, value in fields:
            if value is not None:
                try:
                    if isinstance(field, (EmbeddedDocumentField,
                                          GenericEmbeddedDocumentField)):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError as error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError) as error:
                    errors[field.name] = error
            elif field.required and not getattr(field, '_auto_gen', False):
                errors[field.name] = ValidationError('Field is required',
                                                     field_name=field.name)

        if errors:
            pk = "None"
            if hasattr(self, 'pk'):
                pk = self.pk
            elif self._instance:
                pk = self._instance.pk
            message = "ValidationError (%s:%s) " % (self._class_name, pk)
            raise ValidationError(message, errors=errors)

    def to_json(self):
        """Converts a document to JSON"""
        return json_util.dumps(self.to_mongo())

    @classmethod
    def from_json(cls, json_data):
        """Converts json data to an unsaved document instance"""
        return cls._from_son(json_util.loads(json_data))

    def __expand_dynamic_values(self, name, value):
        """expand any dynamic values to their correct types / values"""
        if not isinstance(value, (dict, list, tuple)):
            return value

        is_list = False
        if not hasattr(value, 'items'):
            is_list = True
            value = dict([(k, v) for k, v in enumerate(value)])

        if not is_list and '_cls' in value:
            cls = get_document(value['_cls'])
            return cls(**value)

        data = {}
        for k, v in list(value.items()):
            key = name if is_list else k
            data[k] = self.__expand_dynamic_values(key, v)

        if is_list:  # Convert back to a list
            data_items = sorted(list(data.items()), key=operator.itemgetter(0))
            value = [v for k, v in data_items]
        else:
            value = data

        # Convert lists / values so we can watch for any changes on them
        if (isinstance(value, (list, tuple)) and
           not isinstance(value, BaseList)):
            value = BaseList(value, self, name)
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, self, name)

        return value

    def _mark_as_changed(self, key):
        """Marks a key as explicitly changed by the user.
        """

        if key:
            self._changed_fields.add(key)

    def _get_changed_fields(self):
        """Returns a list of all fields that have explicitly been changed.
        """
        changed_fields = set(self._changed_fields)
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        for field_name, field in self._fields.items():
            if field_name not in changed_fields:
                if (isinstance(field, ComplexBaseField) and
                   isinstance(field.field, EmbeddedDocumentField)):
                    field_value = getattr(self, field_name, None)
                    if field_value:
                        for idx in (field_value if isinstance(field_value, dict)
                                    else range(len(field_value))):
                            changed_subfields = field_value[idx]._get_changed_fields()
                            if changed_subfields:
                                changed_fields |= set(['.'.join([field_name, str(idx), subfield_name])
                                        for subfield_name in changed_subfields])
                elif isinstance(field, EmbeddedDocumentField):
                    field_value = getattr(self, field_name, None)
                    if field_value:
                        changed_subfields = field_value._get_changed_fields()
                        if changed_subfields:
                            changed_fields |= set(['.'.join([field_name, subfield_name])
                                    for subfield_name in changed_subfields])
        return changed_fields

    def _clear_changed_fields(self):
        _set(self, '_changed_fields', set())
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        for field_name, field in self._fields.items():
            if (isinstance(field, ComplexBaseField) and
               isinstance(field.field, EmbeddedDocumentField)):
                field_value = getattr(self, field_name, None)
                if field_value:
                    for idx in (field_value if isinstance(field_value, dict)
                                else range(len(field_value))):
                        field_value[idx]._clear_changed_fields()
            elif isinstance(field, EmbeddedDocumentField):
                field_value = getattr(self, field_name, None)
                if field_value:
                    field_value._clear_changed_fields()

    def _delta(self, full=False):
        sets = {}
        unsets = {}


        def get_db_value(field, value):
            if value is None:
                value = field.default() if isinstance(field.default, collections.Callable) else field.default
            return field.to_mongo(value)


        if full or not self._created:
            fields = iter(self._fields.items())
            db_data = ((self._db_field_map.get(field_name, field_name),
                    get_db_value(field, getattr(self, field_name)))
                    for field_name, field in fields)

        else:
            # List of (db_field_name, db_value) tuples.
            db_data = []

            for field_name in self._get_changed_fields():
                parts = field_name.split('.')

                db_field_parts = []

                value = self
                for part in parts:
                    if isinstance(value, list) and part.isdigit():
                        db_field_parts.append(part)
                        field = field.field
                        value = value[int(part)]
                    elif isinstance(value, dict):
                        db_field_parts.append(part)
                        field = field.field
                        value = value[part]
                    else: # It's a document
                        obj = value
                        field = obj._fields[part]
                        db_field_parts.append(obj._db_field_map.get(part, part))
                        value = getattr(obj, part)

                db_data.append(('.'.join(db_field_parts), get_db_value(field, value)))

        for db_field_name, db_value in db_data:
            if db_value == None:
                unsets[db_field_name] = 1
            else:
                sets[db_field_name] = db_value

        return sets, unsets

    @classmethod
    def _get_collection_name(cls):
        """Returns the collection name for this class.
        """
        return cls._meta.get('collection', None)

    @classmethod
    def _from_son(cls, son, _auto_dereference=False):
        # get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get('_cls', cls._class_name)

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = get_document(class_name)

        return cls(_son=son)

    @classmethod
    def _build_index_specs(cls, meta_indexes):
        """Generate and merge the full index specs
        """

        geo_indices = cls._geo_indices()
        unique_indices = cls._unique_with_indexes()
        index_specs = [cls._build_index_spec(spec)
                       for spec in meta_indexes]

        def merge_index_specs(index_specs, indices):
            if not indices:
                return index_specs

            spec_fields = [v['fields']
                           for k, v in enumerate(index_specs)]
            # Merge unqiue_indexes with existing specs
            for k, v in enumerate(indices):
                if v['fields'] in spec_fields:
                    index_specs[spec_fields.index(v['fields'])].update(v)
                else:
                    index_specs.append(v)
            return index_specs

        index_specs = merge_index_specs(index_specs, geo_indices)
        index_specs = merge_index_specs(index_specs, unique_indices)
        return index_specs

    @classmethod
    def _build_index_spec(cls, spec):
        """Build a PyMongo index spec from a MongoEngine index spec.
        """
        if isinstance(spec, str):
            spec = {'fields': [spec]}
        elif isinstance(spec, (list, tuple)):
            spec = {'fields': list(spec)}
        elif isinstance(spec, dict):
            spec = dict(spec)

        index_list = []
        direction = None

        # Check to see if we need to include _cls
        allow_inheritance = cls._meta.get('allow_inheritance',
                                          ALLOW_INHERITANCE)
        include_cls = allow_inheritance and not spec.get('sparse', False)

        for key in spec['fields']:
            # If inherited spec continue
            if isinstance(key, (list, tuple)):
                continue

            # ASCENDING from +,
            # DESCENDING from -
            # GEO2D from *
            direction = pymongo.ASCENDING
            if key.startswith("-"):
                direction = pymongo.DESCENDING
            elif key.startswith("*"):
                direction = pymongo.GEO2D
            if key.startswith(("+", "-", "*")):
                key = key[1:]

            # Use real field name, do it manually because we need field
            # objects for the next part (list field checking)
            parts = key.split('.')
            if parts in (['pk'], ['id'], ['_id']):
                key = '_id'
                fields = []
            else:
                fields = cls._lookup_field(parts)
                parts = [field if field == '_id' else field.db_field
                         for field in fields]
                key = '.'.join(parts)
            index_list.append((key, direction))

        # Don't add cls to a geo index
        if include_cls and direction is not pymongo.GEO2D and spec.get('cls', True):
            index_list.insert(0, ('_cls', 1))

        if index_list:
            spec['fields'] = index_list
        if spec.get('sparse', False) and len(spec['fields']) > 1:
            raise ValueError(
                'Sparse indexes can only have one field in them. '
                'See https://jira.mongodb.org/browse/SERVER-2193')

        return spec

    @classmethod
    def _unique_with_indexes(cls, namespace=""):
        """
        Find and set unique indexes
        """
        unique_indexes = []
        for field_name, field in list(cls._fields.items()):
            sparse = False
            # Generate a list of indexes needed by uniqueness constraints
            if field.unique:
                field.required = True
                unique_fields = [field.db_field]

                # Add any unique_with fields to the back of the index spec
                if field.unique_with:
                    if isinstance(field.unique_with, str):
                        field.unique_with = [field.unique_with]

                    # Convert unique_with field names to real field names
                    unique_with = []
                    for other_name in field.unique_with:
                        parts = other_name.split('.')
                        # Lookup real name
                        parts = cls._lookup_field(parts)
                        name_parts = [part.db_field for part in parts]
                        unique_with.append('.'.join(name_parts))
                        # Unique field should be required
                        parts[-1].required = True
                        sparse = (not sparse and
                                  parts[-1].name not in cls.__dict__)
                    unique_fields += unique_with

                # Add the new index to the list
                fields = [("%s%s" % (namespace, f), pymongo.ASCENDING)
                          for f in unique_fields]
                index = {'fields': fields, 'unique': True, 'sparse': sparse}
                unique_indexes.append(index)

            # Grab any embedded document field unique indexes
            if (field.__class__.__name__ == "EmbeddedDocumentField" and
               field.document_type != cls):
                field_namespace = "%s." % field_name
                doc_cls = field.document_type
                unique_indexes += doc_cls._unique_with_indexes(field_namespace)

        return unique_indexes

    @classmethod
    def _geo_indices(cls, inspected=None, parent_field=None):
        inspected = inspected or []
        geo_indices = []
        inspected.append(cls)

        geo_field_type_names = ["EmbeddedDocumentField", "GeoPointField",
                                "PointField", "LineStringField", "PolygonField"]

        geo_field_types = tuple([_import_class(field) for field in geo_field_type_names])

        for field in list(cls._fields.values()):
            if not isinstance(field, geo_field_types):
                continue
            if hasattr(field, 'document_type'):
                field_cls = field.document_type
                if field_cls in inspected:
                    continue
                if hasattr(field_cls, '_geo_indices'):
                    geo_indices += field_cls._geo_indices(inspected, parent_field=field.db_field)
            elif field._geo_index:
                field_name = field.db_field
                if parent_field:
                    field_name = "%s.%s" % (parent_field, field_name)
                geo_indices.append({'fields':
                                   [(field_name, field._geo_index)]})
        return geo_indices

    @classmethod
    def _lookup_field(cls, parts):
        """Lookup a field based on its attribute and return a list containing
        the field's parents and the field.
        """
        if not isinstance(parts, (list, tuple)):
            parts = [parts]
        fields = []
        field = None

        for field_name in parts:
            # Handle ListField indexing:
            if field_name.isdigit():
                new_field = field.field
                fields.append(field_name)
                continue

            if field is None:
                # Look up first field from the document
                if field_name == 'pk':
                    # Deal with "primary key" alias
                    field_name = cls._meta['id_field']
                if field_name in cls._fields:
                    field = cls._fields[field_name]
                #elif cls._dynamic:
                #    DynamicField = _import_class('DynamicField')
                #    field = DynamicField(db_field=field_name)
                else:
                    raise LookUpError('Cannot resolve field "%s"'
                                      % field_name)
            else:
                ReferenceField = _import_class('ReferenceField')
                GenericReferenceField = _import_class('GenericReferenceField')
                if isinstance(field, (ReferenceField, GenericReferenceField)):
                    raise LookUpError('Cannot perform join in mongoDB: %s' %
                                      '__'.join(parts))
                if hasattr(getattr(field, 'field', None), 'lookup_member'):
                    new_field = field.field.lookup_member(field_name)
                else:
                   # Look up subfield on the previous field
                    new_field = field.lookup_member(field_name)
                if not new_field and isinstance(field, ComplexBaseField):
                    fields.append(field_name)
                    continue
                elif not new_field:
                    raise LookUpError('Cannot resolve field "%s"'
                                      % field_name)
                field = new_field  # update field to the new field type
            fields.append(field)
        return fields

    @classmethod
    def _translate_field_name(cls, field, sep='.'):
        """Translate a field attribute name to a database field name.
        """
        parts = field.split(sep)
        parts = [f.db_field for f in cls._lookup_field(parts)]
        return '.'.join(parts)

    def __set_field_display(self):
        """Dynamically set the display value for a field with choices"""
        for attr_name, field in list(self._fields.items()):
            if field.choices:
                setattr(self,
                        'get_%s_display' % attr_name,
                        partial(self.__get_field_display, field=field))

    def __get_field_display(self, field):
        """Returns the display value for a choice field"""
        value = getattr(self, field.name)
        if field.choices and isinstance(field.choices[0], (list, tuple)):
            return dict(field.choices).get(value, value)
        return value
