from mongoengine.base import BaseField


class IntField(BaseField):
    """32-bit integer field."""

    def __init__(self, min_value=None, max_value=None, **kwargs):
        """
        :param min_value: (optional) A min value that will be applied during validation
        :param max_value: (optional) A max value that will be applied during validation
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.BaseField`
        """
        self.min_value, self.max_value = min_value, max_value
        super().__init__(**kwargs)

    def to_python(self, value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            pass
        return value

    def validate(self, value, clean=True):
        try:
            value = int(value)
        except (TypeError, ValueError):
            self.error("%s could not be converted to int" % value)

        if self.min_value is not None and value < self.min_value:
            self.error("Integer value is too small")

        if self.max_value is not None and value > self.max_value:
            self.error("Integer value is too large")

    def prepare_query_value(self, op, value):
        if value is None:
            return value

        return super().prepare_query_value(op, int(value))


__all__ = ("IntField",)
