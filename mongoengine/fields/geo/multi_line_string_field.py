"""GeoJSON MultiLineStringField."""

from mongoengine.base import GeoJsonBaseField


class MultiLineStringField(GeoJsonBaseField):
    """A GeoJSON field storing a list of LineStrings.

    The data is represented as:

    .. code-block:: js

        {'type' : 'MultiLineString' ,
         'coordinates' : [[[x1, y1], [x1, y1] ... [xn, yn]],
                          [[x1, y1], [x1, y1] ... [xn, yn]]]}

    You can either pass a dict with the full information or a list of points.

    Requires mongodb >= 2.6
    """

    _type = "MultiLineString"


__all__ = ("MultiLineStringField",)
