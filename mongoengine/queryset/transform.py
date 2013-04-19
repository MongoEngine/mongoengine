from collections import defaultdict

from bson import SON

from mongoengine.common import _import_class
from mongoengine.errors import InvalidQueryError, LookUpError

__all__ = ('query', 'update')


COMPARISON_OPERATORS = ('ne', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod',
                          'all', 'size', 'exists', 'not')
GEO_OPERATORS        = ('within_distance', 'within_spherical_distance',
                        'within_box', 'within_polygon', 'near', 'near_sphere',
                        'max_distance')
STRING_OPERATORS     = ('contains', 'icontains', 'startswith',
                        'istartswith', 'endswith', 'iendswith',
                        'exact', 'iexact')
CUSTOM_OPERATORS     = ('match',)
MATCH_OPERATORS      = (COMPARISON_OPERATORS + GEO_OPERATORS +
                        STRING_OPERATORS + CUSTOM_OPERATORS)

UPDATE_OPERATORS     = ('set', 'unset', 'inc', 'dec', 'pop', 'push',
                        'push_all', 'pull', 'pull_all', 'add_to_set')


def query(_doc_cls=None, _field_operation=False, **query):
    """Transform a query from Django-style format to Mongo format.
    """
    mongo_query = {}
    merge_query = defaultdict(list)
    for key, value in sorted(query.items()):
        if key == "__raw__":
            mongo_query.update(value)
            continue

        parts = key.split('__')
        indices = [(i, p) for i, p in enumerate(parts) if p.isdigit()]
        parts = [part for part in parts if not part.isdigit()]
        # Check for an operator and transform to mongo-style if there is
        op = None
        if parts[-1] in MATCH_OPERATORS:
            op = parts.pop()

        negate = False
        if parts[-1] == 'not':
            parts.pop()
            negate = True

        if _doc_cls:
            # Switch field names to proper names [set in Field(name='foo')]
            try:
                fields = _doc_cls._lookup_field(parts)
            except Exception, e:
                raise InvalidQueryError(e)
            parts = []

            cleaned_fields = []
            for field in fields:
                append_field = True
                if isinstance(field, basestring):
                    parts.append(field)
                    append_field = False
                else:
                    parts.append(field.db_field)
                if append_field:
                    cleaned_fields.append(field)

            # Convert value to proper value
            field = cleaned_fields[-1]

            singular_ops = [None, 'ne', 'gt', 'gte', 'lt', 'lte', 'not']
            singular_ops += STRING_OPERATORS
            if op in singular_ops:
                if isinstance(field, basestring):
                    if (op in STRING_OPERATORS and
                        isinstance(value, basestring)):
                        StringField = _import_class('StringField')
                        value = StringField.prepare_query_value(op, value)
                    else:
                        value = field
                else:
                    value = field.prepare_query_value(op, value)
            elif op in ('in', 'nin', 'all', 'near'):
                # 'in', 'nin' and 'all' require a list of values
                value = [field.prepare_query_value(op, v) for v in value]

        # if op and op not in COMPARISON_OPERATORS:
        if op:
            if op in GEO_OPERATORS:
                if op == "within_distance":
                    value = {'$within': {'$center': value}}
                elif op == "within_spherical_distance":
                    value = {'$within': {'$centerSphere': value}}
                elif op == "within_polygon":
                    value = {'$within': {'$polygon': value}}
                elif op == "near":
                    value = {'$near': value}
                elif op == "near_sphere":
                    value = {'$nearSphere': value}
                elif op == 'within_box':
                    value = {'$within': {'$box': value}}
                elif op == "max_distance":
                    value = {'$maxDistance': value}
                else:
                    raise NotImplementedError("Geo method '%s' has not "
                                              "been implemented" % op)
            elif op in CUSTOM_OPERATORS:
                if op == 'match':
                    value = {"$elemMatch": value}
                else:
                    NotImplementedError("Custom method '%s' has not "
                                        "been implemented" % op)
            elif op not in STRING_OPERATORS:
                value = {'$' + op: value}

        if negate:
            value = {'$not': value}

        for i, part in indices:
            parts.insert(i, part)
        key = '.'.join(parts)
        if op is None or key not in mongo_query:
            mongo_query[key] = value
        elif key in mongo_query:
            if key in mongo_query and isinstance(mongo_query[key], dict):
                mongo_query[key].update(value)
                # $maxDistance needs to come last - convert to SON
                if '$maxDistance' in mongo_query[key]:
                    value_dict = mongo_query[key]
                    value_son = SON()
                    for k, v in value_dict.iteritems():
                        if k == '$maxDistance':
                            continue
                        value_son[k] = v
                    value_son['$maxDistance'] = value_dict['$maxDistance']
                    mongo_query[key] = value_son
            else:
                # Store for manually merging later
                merge_query[key].append(value)

    # The queryset has been filter in such a way we must manually merge
    for k, v in merge_query.items():
        merge_query[k].append(mongo_query[k])
        del mongo_query[k]
        if isinstance(v, list):
            value = [{k:val} for val in v]
            if '$and' in mongo_query.keys():
                mongo_query['$and'].append(value)
            else:
                mongo_query['$and'] = value

    return mongo_query


def update(_doc_cls=None, **update):
    """Transform an update spec from Django-style format to Mongo format.
    """
    mongo_update = {}
    for key, value in update.items():
        if key == "__raw__":
            mongo_update.update(value)
            continue
        parts = key.split('__')
        # Check for an operator and transform to mongo-style if there is
        op = None
        if parts[0] in UPDATE_OPERATORS:
            op = parts.pop(0)
            # Convert Pythonic names to Mongo equivalents
            if op in ('push_all', 'pull_all'):
                op = op.replace('_all', 'All')
            elif op == 'dec':
                # Support decrement by flipping a positive value's sign
                # and using 'inc'
                op = 'inc'
                if value > 0:
                    value = -value
            elif op == 'add_to_set':
                op = op.replace('_to_set', 'ToSet')

        match = None
        if parts[-1] in COMPARISON_OPERATORS:
            match = parts.pop()

        if _doc_cls:
            # Switch field names to proper names [set in Field(name='foo')]
            try:
                fields = _doc_cls._lookup_field(parts)
            except Exception, e:
                raise InvalidQueryError(e)
            parts = []

            cleaned_fields = []
            for field in fields:
                append_field = True
                if isinstance(field, basestring):
                    # Convert the S operator to $
                    if field == 'S':
                        field = '$'
                    parts.append(field)
                    append_field = False
                else:
                    parts.append(field.db_field)
                if append_field:
                    cleaned_fields.append(field)

            # Convert value to proper value
            field = cleaned_fields[-1]

            if op in (None, 'set', 'push', 'pull'):
                if field.required or value is not None:
                    value = field.prepare_query_value(op, value)
            elif op in ('pushAll', 'pullAll'):
                value = [field.prepare_query_value(op, v) for v in value]
            elif op == 'addToSet':
                if isinstance(value, (list, tuple, set)):
                    value = [field.prepare_query_value(op, v) for v in value]
                elif field.required or value is not None:
                    value = field.prepare_query_value(op, value)

        if match:
            match = '$' + match
            value = {match: value}

        key = '.'.join(parts)

        if not op:
            raise InvalidQueryError("Updates must supply an operation "
                                    "eg: set__FIELD=value")

        if 'pull' in op and '.' in key:
            # Dot operators don't work on pull operations
            # it uses nested dict syntax
            if op == 'pullAll':
                raise InvalidQueryError("pullAll operations only support "
                                        "a single field depth")

            parts.reverse()
            for key in parts:
                value = {key: value}
        elif op == 'addToSet' and isinstance(value, list):
            value = {key: {"$each": value}}
        else:
            value = {key: value}
        key = '$' + op

        if key not in mongo_update:
            mongo_update[key] = value
        elif key in mongo_update and isinstance(mongo_update[key], dict):
            mongo_update[key].update(value)

    return mongo_update
