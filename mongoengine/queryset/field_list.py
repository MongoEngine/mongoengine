
__all__ = ('QueryFieldList',)


class QueryFieldList(object):
    """Object that handles combinations of .only() and .exclude() calls"""
    ONLY = 1
    EXCLUDE = 0

    def __init__(self, fields=[], value=ONLY, always_include=[]):
        self.value = value
        self.fields = set(fields)
        self.always_include = set(always_include)
        self._id = None

    def __add__(self, f):
        if not self.fields:
            self.fields = f.fields
            self.value = f.value
        elif self.value is self.ONLY and f.value is self.ONLY:
            self.fields = self.fields.intersection(f.fields)
        elif self.value is self.EXCLUDE and f.value is self.EXCLUDE:
            self.fields = self.fields.union(f.fields)
        elif self.value is self.ONLY and f.value is self.EXCLUDE:
            self.fields -= f.fields
        elif self.value is self.EXCLUDE and f.value is self.ONLY:
            self.value = self.ONLY
            self.fields = f.fields - self.fields

        if '_id' in f.fields:
            self._id = f.value

        if self.always_include:
            if self.value is self.ONLY and self.fields:
                self.fields = self.fields.union(self.always_include)
            else:
                self.fields -= self.always_include
        return self

    def __nonzero__(self):
        return bool(self.fields)

    def as_dict(self):
        field_list = dict((field, self.value) for field in self.fields)
        if self._id is not None:
            field_list['_id'] = self._id
        return field_list

    def reset(self):
        self.fields = set([])
        self.value = self.ONLY
