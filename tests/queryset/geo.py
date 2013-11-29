import sys
sys.path[0:0] = [""]

import unittest
from datetime import datetime, timedelta
from mongoengine import *

__all__ = ("GeoQueriesTest",)


class GeoQueriesTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')

    def test_geospatial_operators(self):
        """Ensure that geospatial queries are working.
        """
        class Event(Document):
            title = StringField()
            date = DateTimeField()
            location = GeoPointField()

            def __unicode__(self):
                return self.title

        Event.drop_collection()

        event1 = Event(title="Coltrane Motion @ Double Door",
                       date=datetime.now() - timedelta(days=1),
                       location=[-87.677137, 41.909889]).save()
        event2 = Event(title="Coltrane Motion @ Bottom of the Hill",
                       date=datetime.now() - timedelta(days=10),
                       location=[-122.4194155, 37.7749295]).save()
        event3 = Event(title="Coltrane Motion @ Empty Bottle",
                       date=datetime.now(),
                       location=[-87.686638, 41.900474]).save()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = Event.objects(location__near=[-87.67892, 41.9120459])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

        # find events within 5 degrees of pitchfork office, chicago
        point_and_distance = [[-87.67892, 41.9120459], 5]
        events = Event.objects(location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 2)
        events = list(events)
        self.assertTrue(event2 not in events)
        self.assertTrue(event1 in events)
        self.assertTrue(event3 in events)

        # ensure ordering is respected by "near"
        events = Event.objects(location__near=[-87.67892, 41.9120459])
        events = events.order_by("-date")
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event3, event1, event2])

        # find events within 10 degrees of san francisco
        point = [-122.415579, 37.7566023]
        events = Event.objects(location__near=point, location__max_distance=10)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

        # find events within 10 degrees of san francisco
        point_and_distance = [[-122.415579, 37.7566023], 10]
        events = Event.objects(location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

        # find events within 1 degree of greenpoint, broolyn, nyc, ny
        point_and_distance = [[-73.9509714, 40.7237134], 1]
        events = Event.objects(location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 0)

        # ensure ordering is respected by "within_distance"
        point_and_distance = [[-87.67892, 41.9120459], 10]
        events = Event.objects(location__within_distance=point_and_distance)
        events = events.order_by("-date")
        self.assertEqual(events.count(), 2)
        self.assertEqual(events[0], event3)

        # check that within_box works
        box = [(-125.0, 35.0), (-100.0, 40.0)]
        events = Event.objects(location__within_box=box)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event2.id)

        polygon = [
            (-87.694445, 41.912114),
            (-87.69084, 41.919395),
            (-87.681742, 41.927186),
            (-87.654276, 41.911731),
            (-87.656164, 41.898061),
        ]
        events = Event.objects(location__within_polygon=polygon)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event1.id)

        polygon2 = [
            (-1.742249, 54.033586),
            (-1.225891, 52.792797),
            (-4.40094, 53.389881)
        ]
        events = Event.objects(location__within_polygon=polygon2)
        self.assertEqual(events.count(), 0)

    def test_geo_spatial_embedded(self):

        class Venue(EmbeddedDocument):
            location = GeoPointField()
            name = StringField()

        class Event(Document):
            title = StringField()
            venue = EmbeddedDocumentField(Venue)

        Event.drop_collection()

        venue1 = Venue(name="The Rock", location=[-87.677137, 41.909889])
        venue2 = Venue(name="The Bridge", location=[-122.4194155, 37.7749295])

        event1 = Event(title="Coltrane Motion @ Double Door",
                       venue=venue1).save()
        event2 = Event(title="Coltrane Motion @ Bottom of the Hill",
                       venue=venue2).save()
        event3 = Event(title="Coltrane Motion @ Empty Bottle",
                       venue=venue1).save()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = Event.objects(venue__location__near=[-87.67892, 41.9120459])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

    def test_spherical_geospatial_operators(self):
        """Ensure that spherical geospatial queries are working
        """
        class Point(Document):
            location = GeoPointField()

        Point.drop_collection()

        # These points are one degree apart, which (according to Google Maps)
        # is about 110 km apart at this place on the Earth.
        north_point = Point(location=[-122, 38]).save()  # Near Concord, CA
        south_point = Point(location=[-122, 37]).save()  # Near Santa Cruz, CA

        earth_radius = 6378.009  # in km (needs to be a float for dividing by)

        # Finds both points because they are within 60 km of the reference
        # point equidistant between them.
        points = Point.objects(location__near_sphere=[-122, 37.5])
        self.assertEqual(points.count(), 2)

        # Same behavior for _within_spherical_distance
        points = Point.objects(
            location__within_spherical_distance=[[-122, 37.5], 60/earth_radius]
        )
        self.assertEqual(points.count(), 2)

        points = Point.objects(location__near_sphere=[-122, 37.5],
                               location__max_distance=60 / earth_radius)
        self.assertEqual(points.count(), 2)

        # Finds both points, but orders the north point first because it's
        # closer to the reference point to the north.
        points = Point.objects(location__near_sphere=[-122, 38.5])
        self.assertEqual(points.count(), 2)
        self.assertEqual(points[0].id, north_point.id)
        self.assertEqual(points[1].id, south_point.id)

        # Finds both points, but orders the south point first because it's
        # closer to the reference point to the south.
        points = Point.objects(location__near_sphere=[-122, 36.5])
        self.assertEqual(points.count(), 2)
        self.assertEqual(points[0].id, south_point.id)
        self.assertEqual(points[1].id, north_point.id)

        # Finds only one point because only the first point is within 60km of
        # the reference point to the south.
        points = Point.objects(
            location__within_spherical_distance=[[-122, 36.5], 60/earth_radius])
        self.assertEqual(points.count(), 1)
        self.assertEqual(points[0].id, south_point.id)

    def test_2dsphere_point(self):

        class Event(Document):
            title = StringField()
            date = DateTimeField()
            location = PointField()

            def __unicode__(self):
                return self.title

        Event.drop_collection()

        event1 = Event(title="Coltrane Motion @ Double Door",
                       date=datetime.now() - timedelta(days=1),
                       location=[-87.677137, 41.909889])
        event1.save()
        event2 = Event(title="Coltrane Motion @ Bottom of the Hill",
                       date=datetime.now() - timedelta(days=10),
                       location=[-122.4194155, 37.7749295]).save()
        event3 = Event(title="Coltrane Motion @ Empty Bottle",
                       date=datetime.now(),
                       location=[-87.686638, 41.900474]).save()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = Event.objects(location__near=[-87.67892, 41.9120459])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

        # find events within 5 degrees of pitchfork office, chicago
        point_and_distance = [[-87.67892, 41.9120459], 2]
        events = Event.objects(location__geo_within_center=point_and_distance)
        self.assertEqual(events.count(), 2)
        events = list(events)
        self.assertTrue(event2 not in events)
        self.assertTrue(event1 in events)
        self.assertTrue(event3 in events)

        # ensure ordering is respected by "near"
        events = Event.objects(location__near=[-87.67892, 41.9120459])
        events = events.order_by("-date")
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event3, event1, event2])

        # find events within 10km of san francisco
        point = [-122.415579, 37.7566023]
        events = Event.objects(location__near=point, location__max_distance=10000)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

        # find events within 1km of greenpoint, broolyn, nyc, ny
        events = Event.objects(location__near=[-73.9509714, 40.7237134], location__max_distance=1000)
        self.assertEqual(events.count(), 0)

        # ensure ordering is respected by "near"
        events = Event.objects(location__near=[-87.67892, 41.9120459],
                               location__max_distance=10000).order_by("-date")
        self.assertEqual(events.count(), 2)
        self.assertEqual(events[0], event3)

        # check that within_box works
        box = [(-125.0, 35.0), (-100.0, 40.0)]
        events = Event.objects(location__geo_within_box=box)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event2.id)

        polygon = [
            (-87.694445, 41.912114),
            (-87.69084, 41.919395),
            (-87.681742, 41.927186),
            (-87.654276, 41.911731),
            (-87.656164, 41.898061),
        ]
        events = Event.objects(location__geo_within_polygon=polygon)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event1.id)

        polygon2 = [
            (-1.742249, 54.033586),
            (-1.225891, 52.792797),
            (-4.40094, 53.389881)
        ]
        events = Event.objects(location__geo_within_polygon=polygon2)
        self.assertEqual(events.count(), 0)

    def test_2dsphere_point_embedded(self):

        class Venue(EmbeddedDocument):
            location = GeoPointField()
            name = StringField()

        class Event(Document):
            title = StringField()
            venue = EmbeddedDocumentField(Venue)

        Event.drop_collection()

        venue1 = Venue(name="The Rock", location=[-87.677137, 41.909889])
        venue2 = Venue(name="The Bridge", location=[-122.4194155, 37.7749295])

        event1 = Event(title="Coltrane Motion @ Double Door",
                       venue=venue1).save()
        event2 = Event(title="Coltrane Motion @ Bottom of the Hill",
                       venue=venue2).save()
        event3 = Event(title="Coltrane Motion @ Empty Bottle",
                       venue=venue1).save()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = Event.objects(venue__location__near=[-87.67892, 41.9120459])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

    def test_linestring(self):

        class Road(Document):
            name = StringField()
            line = LineStringField()

        Road.drop_collection()

        Road(name="66", line=[[40, 5], [41, 6]]).save()

        # near
        point = {"type": "Point", "coordinates": [40, 5]}
        roads = Road.objects.filter(line__near=point["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__near=point).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__near={"$geometry": point}).count()
        self.assertEqual(1, roads)

        # Within
        polygon = {"type": "Polygon",
                   "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        roads = Road.objects.filter(line__geo_within=polygon["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__geo_within=polygon).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__geo_within={"$geometry": polygon}).count()
        self.assertEqual(1, roads)

        # Intersects
        line = {"type": "LineString",
                "coordinates": [[40, 5], [40, 6]]}
        roads = Road.objects.filter(line__geo_intersects=line["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__geo_intersects=line).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__geo_intersects={"$geometry": line}).count()
        self.assertEqual(1, roads)

        polygon = {"type": "Polygon",
                   "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        roads = Road.objects.filter(line__geo_intersects=polygon["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__geo_intersects=polygon).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(line__geo_intersects={"$geometry": polygon}).count()
        self.assertEqual(1, roads)

    def test_polygon(self):

        class Road(Document):
            name = StringField()
            poly = PolygonField()

        Road.drop_collection()

        Road(name="66", poly=[[[40, 5], [40, 6], [41, 6], [40, 5]]]).save()

        # near
        point = {"type": "Point", "coordinates": [40, 5]}
        roads = Road.objects.filter(poly__near=point["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__near=point).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__near={"$geometry": point}).count()
        self.assertEqual(1, roads)

        # Within
        polygon = {"type": "Polygon",
                   "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        roads = Road.objects.filter(poly__geo_within=polygon["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__geo_within=polygon).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__geo_within={"$geometry": polygon}).count()
        self.assertEqual(1, roads)

        # Intersects
        line = {"type": "LineString",
                "coordinates": [[40, 5], [41, 6]]}
        roads = Road.objects.filter(poly__geo_intersects=line["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__geo_intersects=line).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__geo_intersects={"$geometry": line}).count()
        self.assertEqual(1, roads)

        polygon = {"type": "Polygon",
                   "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        roads = Road.objects.filter(poly__geo_intersects=polygon["coordinates"]).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__geo_intersects=polygon).count()
        self.assertEqual(1, roads)

        roads = Road.objects.filter(poly__geo_intersects={"$geometry": polygon}).count()
        self.assertEqual(1, roads)

    def test_2dsphere_point_sets_correctly(self):
        class Location(Document):
            loc = PointField()

        Location.drop_collection()

        Location(loc=[1,2]).save()
        loc = Location.objects.as_pymongo()[0]
        self.assertEqual(loc["loc"], {"type": "Point", "coordinates": [1, 2]})

        Location.objects.update(set__loc=[2,1])
        loc = Location.objects.as_pymongo()[0]
        self.assertEqual(loc["loc"], {"type": "Point", "coordinates": [2, 1]})

    def test_2dsphere_linestring_sets_correctly(self):
        class Location(Document):
            line = LineStringField()

        Location.drop_collection()

        Location(line=[[1, 2], [2, 2]]).save()
        loc = Location.objects.as_pymongo()[0]
        self.assertEqual(loc["line"], {"type": "LineString", "coordinates": [[1, 2], [2, 2]]})

        Location.objects.update(set__line=[[2, 1], [1, 2]])
        loc = Location.objects.as_pymongo()[0]
        self.assertEqual(loc["line"], {"type": "LineString", "coordinates": [[2, 1], [1, 2]]})

    def test_geojson_PolygonField(self):
        class Location(Document):
            poly = PolygonField()

        Location.drop_collection()

        Location(poly=[[[40, 5], [40, 6], [41, 6], [40, 5]]]).save()
        loc = Location.objects.as_pymongo()[0]
        self.assertEqual(loc["poly"], {"type": "Polygon", "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]]})

        Location.objects.update(set__poly=[[[40, 4], [40, 6], [41, 6], [40, 4]]])
        loc = Location.objects.as_pymongo()[0]
        self.assertEqual(loc["poly"], {"type": "Polygon", "coordinates": [[[40, 4], [40, 6], [41, 6], [40, 4]]]})

if __name__ == '__main__':
    unittest.main()
