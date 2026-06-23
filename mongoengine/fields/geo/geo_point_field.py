import pymongo

from mongoengine.base import BaseField


class GeoPointField(BaseField):
    """A list storing a longitude and latitude coordinate.

    .. note:: this represents a generic point in a 2D plane and a legacy way of
        representing a geo point. It admits 2d indexes but not "2dsphere" indexes
        in MongoDB > 2.4 which are more natural for modeling geospatial points.
        See :ref:`geospatial-indexes`
    """

    _geo_index = pymongo.GEO2D

    def validate(self, value, clean=True):
        """Make sure that a geo-value is of type (x, y)"""
        if not isinstance(value, (list, tuple)):
            self.error("GeoPointField can only accept tuples or lists of (x, y)")

        if not len(value) == 2:
            self.error("Value (%s) must be a two-dimensional point" % repr(value))
        elif not isinstance(value[0], (float, int)) or not isinstance(
            value[1], (float, int)
        ):
            self.error("Both values (%s) in point must be float or int" % repr(value))


__all__ = ("GeoPointField",)
