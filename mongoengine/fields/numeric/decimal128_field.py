import decimal

from bson.decimal128 import Decimal128, create_decimal128_context

from mongoengine.base import BaseField


class Decimal128Field(BaseField):
    """
    128-bit decimal-based floating-point field capable of emulating decimal
    rounding with exact precision. This field will expose decimal.Decimal but stores the value as a
    `bson.Decimal128` behind the scene, this field is intended for monetary data, scientific computations, etc.
    """

    DECIMAL_CONTEXT = create_decimal128_context()

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        super().__init__(**kwargs)

    def to_mongo(self, value):
        if value is None:
            return None
        if isinstance(value, Decimal128):
            return value
        if not isinstance(value, decimal.Decimal):
            with decimal.localcontext(self.DECIMAL_CONTEXT) as ctx:
                value = ctx.create_decimal(value)
        return Decimal128(value)

    def to_python(self, value):
        if value is None:
            return None
        return self.to_mongo(value).to_decimal()

    def validate(self, value, clean=True):
        if not isinstance(value, Decimal128):
            try:
                value = Decimal128(value)
            except (TypeError, ValueError, decimal.InvalidOperation) as exc:
                self.error("Could not convert value to Decimal128: %s" % exc)

        if self.min_value is not None and value.to_decimal() < self.min_value:
            self.error("Decimal value is too small")

        if self.max_value is not None and value.to_decimal() > self.max_value:
            self.error("Decimal value is too large")

    def prepare_query_value(self, op, value):
        return super().prepare_query_value(op, self.to_mongo(value))


__all__ = ("Decimal128Field",)
