
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
        self.slice = {}

    def __add__(self, f):
        if isinstance(f.value, dict):
            for field in f.fields:
                self.slice[field] = f.value
            if not self.fields:
                self.fields = f.fields
        elif not self.fields:
            self.fields = f.fields
            self.value = f.value
            self.slice = {}
        elif self.value is self.ONLY and f.value is self.ONLY:
            self._clean_slice()
            self.fields = self.fields.intersection(f.fields)
        elif self.value is self.EXCLUDE and f.value is self.EXCLUDE:
            self.fields = self.fields.union(f.fields)
            self._clean_slice()
        elif self.value is self.ONLY and f.value is self.EXCLUDE:
            self.fields -= f.fields
            self._clean_slice()
        elif self.value is self.EXCLUDE and f.value is self.ONLY:
            self.value = self.ONLY
            self.fields = f.fields - self.fields
            self._clean_slice()

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
        if self.slice:
            field_list.update(self.slice)
        if self._id is not None:
            field_list['_id'] = self._id
        return field_list

    def reset(self):
        self.fields = set([])
        self.slice = {}
        self.value = self.ONLY

    def _clean_slice(self):
        if self.slice:
            for field in set(self.slice.keys()) - self.fields:
                del self.slice[field]
