import decimal

from mongoengine.base import BaseField


class DecimalField(BaseField):
    """Disclaimer: This field is kept for historical reason but since it converts the values to float, it
    is not suitable for true decimal storage. Consider using :class:`~mongoengine.fields.Decimal128Field`.

    Fixed-point decimal number field. Stores the value as a float by default unless `force_string` is used.
    If using floats, beware of Decimal to float conversion (potential precision loss)
    """

    def __init__(
        self,
        min_value=None,
        max_value=None,
        force_string=False,
        precision=2,
        rounding=decimal.ROUND_HALF_UP,
        **kwargs,
    ):
        """
        :param min_value: (optional) A min value that will be applied during validation
        :param max_value: (optional) A max value that will be applied during validation
        :param force_string: Store the value as a string (instead of a float).
         Be aware that this affects query sorting and operation like lte, gte (as string comparison is applied)
         and some query operator won't work (e.g. inc, dec)
        :param precision: Number of decimal places to store.
        :param rounding: The rounding rule from the python decimal library:

            - decimal.ROUND_CEILING (towards Infinity)
            - decimal.ROUND_DOWN (towards zero)
            - decimal.ROUND_FLOOR (towards -Infinity)
            - decimal.ROUND_HALF_DOWN (to nearest with ties going towards zero)
            - decimal.ROUND_HALF_EVEN (to nearest with ties going to nearest even integer)
            - decimal.ROUND_HALF_UP (to nearest with ties going away from zero)
            - decimal.ROUND_UP (away from zero)
            - decimal.ROUND_05UP (away from zero if last digit after rounding towards zero would have been 0 or 5; otherwise towards zero)

            Defaults to: ``decimal.ROUND_HALF_UP``
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.BaseField`
        """
        self.min_value = min_value
        self.max_value = max_value
        self.force_string = force_string

        if precision < 0 or not isinstance(precision, int):
            self.error("precision must be a positive integer")

        self.precision = precision
        self.rounding = rounding

        super().__init__(**kwargs)

    def to_python(self, value):
        # Convert to string for python 2.6 before casting to Decimal
        try:
            value = decimal.Decimal("%s" % value)
        except (TypeError, ValueError, decimal.InvalidOperation):
            return value
        if self.precision > 0:
            return value.quantize(
                decimal.Decimal(".%s" % ("0" * self.precision)), rounding=self.rounding
            )
        else:
            return value.quantize(decimal.Decimal(), rounding=self.rounding)

    def to_mongo(self, value):
        if self.force_string:
            return str(self.to_python(value))
        return float(self.to_python(value))

    def validate(self, value, clean=True):
        if not isinstance(value, decimal.Decimal):
            if not isinstance(value, str):
                value = str(value)
            try:
                value = decimal.Decimal(value)
            except (TypeError, ValueError, decimal.InvalidOperation) as exc:
                self.error("Could not convert value to decimal: %s" % exc)

        if self.min_value is not None and value < self.min_value:
            self.error("Decimal value is too small")

        if self.max_value is not None and value > self.max_value:
            self.error("Decimal value is too large")

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        return super().prepare_query_value(op, self.to_mongo(value))


__all__ = ("DecimalField",)
