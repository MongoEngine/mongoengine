import pymongo
from bson import SON

from .base_field import BaseField


class GeoJsonBaseField(BaseField):
    """A geo json field storing a geojson style object."""

    _geo_index = pymongo.GEOSPHERE
    _type = "GeoBase"

    def __init__(self, auto_index=True, *args, **kwargs):
        """
        :param bool auto_index: Automatically create a '2dsphere' index.\
            Defaults to `True`.
        """
        self._name = "%sField" % self._type
        if not auto_index:
            self._geo_index = False
        super().__init__(*args, **kwargs)

    def validate(self, value, clean=True):
        """Validate the GeoJson object based on its type."""
        if isinstance(value, dict):
            if set(value.keys()) == {"type", "coordinates"}:
                if value["type"] != self._type:
                    self.error(f'{self._name} type must be "{self._type}"')
                return self.validate(value["coordinates"])
            else:
                self.error(
                    "%s can only accept a valid GeoJson dictionary"
                    " or lists of (x, y)" % self._name
                )
                return
        elif not isinstance(value, (list, tuple)):
            self.error("%s can only accept lists of [x, y]" % self._name)
            return

        validate = getattr(self, "_validate_%s" % self._type.lower())
        error = validate(value)
        if error:
            self.error(error)

    def _validate_polygon(self, value, top_level=True):
        if not isinstance(value, (list, tuple)):
            return "Polygons must contain list of linestrings"

        # Quick and dirty validator
        try:
            value[0][0][0]
        except (TypeError, IndexError):
            return "Invalid Polygon must contain at least one valid linestring"

        errors = []
        for val in value:
            error = self._validate_linestring(val, False)
            if not error and val[0] != val[-1]:
                error = "LineStrings must start and end at the same point"
            if error and error not in errors:
                errors.append(error)
        if errors:
            if top_level:
                return "Invalid Polygon:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_linestring(self, value, top_level=True):
        """Validate a linestring."""
        if not isinstance(value, (list, tuple)):
            return "LineStrings must contain list of coordinate pairs"

        # Quick and dirty validator
        try:
            value[0][0]
        except (TypeError, IndexError):
            return "Invalid LineString must contain at least one valid point"

        errors = []
        for val in value:
            error = self._validate_point(val)
            if error and error not in errors:
                errors.append(error)
        if errors:
            if top_level:
                return "Invalid LineString:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_point(self, value):
        """Validate each set of coords"""
        if not isinstance(value, (list, tuple)):
            return "Points must be a list of coordinate pairs"
        elif not len(value) == 2:
            return "Value (%s) must be a two-dimensional point" % repr(value)
        elif not isinstance(value[0], (float, int)) or not isinstance(
            value[1], (float, int)
        ):
            return "Both values (%s) in point must be float or int" % repr(value)

    def _validate_multipoint(self, value):
        if not isinstance(value, (list, tuple)):
            return "MultiPoint must be a list of Point"

        # Quick and dirty validator
        try:
            value[0][0]
        except (TypeError, IndexError):
            return "Invalid MultiPoint must contain at least one valid point"

        errors = []
        for point in value:
            error = self._validate_point(point)
            if error and error not in errors:
                errors.append(error)

        if errors:
            return "%s" % ", ".join(errors)

    def _validate_multilinestring(self, value, top_level=True):
        if not isinstance(value, (list, tuple)):
            return "MultiLineString must be a list of LineString"

        # Quick and dirty validator
        try:
            value[0][0][0]
        except (TypeError, IndexError):
            return "Invalid MultiLineString must contain at least one valid linestring"

        errors = []
        for linestring in value:
            error = self._validate_linestring(linestring, False)
            if error and error not in errors:
                errors.append(error)

        if errors:
            if top_level:
                return "Invalid MultiLineString:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_multipolygon(self, value):
        if not isinstance(value, (list, tuple)):
            return "MultiPolygon must be a list of Polygon"

        # Quick and dirty validator
        try:
            value[0][0][0][0]
        except (TypeError, IndexError):
            return "Invalid MultiPolygon must contain at least one valid Polygon"

        errors = []
        for polygon in value:
            error = self._validate_polygon(polygon, False)
            if error and error not in errors:
                errors.append(error)

        if errors:
            return "Invalid MultiPolygon:\n%s" % ", ".join(errors)

    def to_mongo(self, value):
        if isinstance(value, dict):
            return value
        return SON([("type", self._type), ("coordinates", value)])


__all__ = ("GeoJsonBaseField",)
