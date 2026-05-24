from bson import ObjectId

from .base_field import BaseField


class ObjectIdField(BaseField):
    """A field wrapper around MongoDB's ObjectIds."""

    def to_python(self, value):
        try:
            if not isinstance(value, ObjectId):
                value = ObjectId(value)
        except Exception:
            pass
        return value

    def to_mongo(self, value):
        if isinstance(value, ObjectId):
            return value

        try:
            return ObjectId(str(value))
        except Exception as e:
            self.error(str(e))

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        return self.to_mongo(value)

    def validate(self, value, clean=True):
        try:
            ObjectId(str(value))
        except Exception:
            self.error("Invalid ObjectID")


__all__ = ("ObjectIdField",)
