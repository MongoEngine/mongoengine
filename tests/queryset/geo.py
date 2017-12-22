import datetime
import unittest

from mongoengine import *

from tests.utils import MongoDBTestCase, needs_mongodb_v3


__all__ = ("GeoQueriesTest",)


class GeoQueriesTest(MongoDBTestCase):

    def _create_event_data(self, point_field_class=GeoPointField):
        """Create some sample data re-used in many of the tests below."""
        class Event(Document):
            title = StringField()
            date = DateTimeField()
            location = point_field_class()

            def __unicode__(self):
                return self.title

        self.Event = Event

        Event.drop_collection()

        event1 = Event.objects.create(
            title="Coltrane Motion @ Double Door",
            date=datetime.datetime.now() - datetime.timedelta(days=1),
            location=[-87.677137, 41.909889])
        event2 = Event.objects.create(
            title="Coltrane Motion @ Bottom of the Hill",
            date=datetime.datetime.now() - datetime.timedelta(days=10),
            location=[-122.4194155, 37.7749295])
        event3 = Event.objects.create(
            title="Coltrane Motion @ Empty Bottle",
            date=datetime.datetime.now(),
            location=[-87.686638, 41.900474])

        return event1, event2, event3

    def test_near(self):
        """Make sure the "near" operator works."""
        event1, event2, event3 = self._create_event_data()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = self.Event.objects(location__near=[-87.67892, 41.9120459])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

        # ensure ordering is respected by "near"
        events = self.Event.objects(location__near=[-87.67892, 41.9120459])
        events = events.order_by("-date")
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event3, event1, event2])

    def test_near_and_max_distance(self):
        """Ensure the "max_distance" operator works alongside the "near"
        operator.
        """
        event1, event2, event3 = self._create_event_data()

        # find events within 10 degrees of san francisco
        point = [-122.415579, 37.7566023]
        events = self.Event.objects(location__near=point,
                                    location__max_distance=10)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

    # $minDistance was added in MongoDB v2.6, but continued being buggy
    # until v3.0; skip for older versions
    @needs_mongodb_v3
    def test_near_and_min_distance(self):
        """Ensure the "min_distance" operator works alongside the "near"
        operator.
        """
        event1, event2, event3 = self._create_event_data()

        # find events at least 10 degrees away of san francisco
        point = [-122.415579, 37.7566023]
        events = self.Event.objects(location__near=point,
                                    location__min_distance=10)
        self.assertEqual(events.count(), 2)

    def test_within_distance(self):
        """Make sure the "within_distance" operator works."""
        event1, event2, event3 = self._create_event_data()

        # find events within 5 degrees of pitchfork office, chicago
        point_and_distance = [[-87.67892, 41.9120459], 5]
        events = self.Event.objects(
            location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 2)
        events = list(events)
        self.assertTrue(event2 not in events)
        self.assertTrue(event1 in events)
        self.assertTrue(event3 in events)

        # find events within 10 degrees of san francisco
        point_and_distance = [[-122.415579, 37.7566023], 10]
        events = self.Event.objects(
            location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

        # find events within 1 degree of greenpoint, broolyn, nyc, ny
        point_and_distance = [[-73.9509714, 40.7237134], 1]
        events = self.Event.objects(
            location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 0)

        # ensure ordering is respected by "within_distance"
        point_and_distance = [[-87.67892, 41.9120459], 10]
        events = self.Event.objects(
            location__within_distance=point_and_distance)
        events = events.order_by("-date")
        self.assertEqual(events.count(), 2)
        self.assertEqual(events[0], event3)

    def test_within_box(self):
        """Ensure the "within_box" operator works."""
        event1, event2, event3 = self._create_event_data()

        # check that within_box works
        box = [(-125.0, 35.0), (-100.0, 40.0)]
        events = self.Event.objects(location__within_box=box)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event2.id)

    def test_within_polygon(self):
        """Ensure the "within_polygon" operator works."""
        event1, event2, event3 = self._create_event_data()

        polygon = [
            (-87.694445, 41.912114),
            (-87.69084, 41.919395),
            (-87.681742, 41.927186),
            (-87.654276, 41.911731),
            (-87.656164, 41.898061),
        ]
        events = self.Event.objects(location__within_polygon=polygon)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event1.id)

        polygon2 = [
            (-1.742249, 54.033586),
            (-1.225891, 52.792797),
            (-4.40094, 53.389881)
        ]
        events = self.Event.objects(location__within_polygon=polygon2)
        self.assertEqual(events.count(), 0)

    def test_2dsphere_near(self):
        """Make sure the "near" operator works with a PointField, which
        corresponds to a 2dsphere index.
        """
        event1, event2, event3 = self._create_event_data(
            point_field_class=PointField
        )

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = self.Event.objects(location__near=[-87.67892, 41.9120459])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

        # ensure ordering is respected by "near"
        events = self.Event.objects(location__near=[-87.67892, 41.9120459])
        events = events.order_by("-date")
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event3, event1, event2])

    def test_2dsphere_near_and_max_distance(self):
        """Ensure the "max_distance" operator works alongside the "near"
        operator with a 2dsphere index.
        """
        event1, event2, event3 = self._create_event_data(
            point_field_class=PointField
        )

        # find events within 10km of san francisco
        point = [-122.415579, 37.7566023]
        events = self.Event.objects(location__near=point,
                                    location__max_distance=10000)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

        # find events within 1km of greenpoint, broolyn, nyc, ny
        events = self.Event.objects(location__near=[-73.9509714, 40.7237134],
                                    location__max_distance=1000)
        self.assertEqual(events.count(), 0)

        # ensure ordering is respected by "near"
        events = self.Event.objects(
            location__near=[-87.67892, 41.9120459],
            location__max_distance=10000
        ).order_by("-date")
        self.assertEqual(events.count(), 2)
        self.assertEqual(events[0], event3)

    def test_2dsphere_geo_within_box(self):
        """Ensure the "geo_within_box" operator works with a 2dsphere
        index.
        """
        event1, event2, event3 = self._create_event_data(
            point_field_class=PointField
        )

        # check that within_box works
        box = [(-125.0, 35.0), (-100.0, 40.0)]
        events = self.Event.objects(location__geo_within_box=box)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event2.id)

    def test_2dsphere_geo_within_polygon(self):
        """Ensure the "geo_within_polygon" operator works with a
        2dsphere index.
        """
        event1, event2, event3 = self._create_event_data(
            point_field_class=PointField
        )

        polygon = [
            (-87.694445, 41.912114),
            (-87.69084, 41.919395),
            (-87.681742, 41.927186),
            (-87.654276, 41.911731),
            (-87.656164, 41.898061),
        ]
        events = self.Event.objects(location__geo_within_polygon=polygon)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event1.id)

        polygon2 = [
            (-1.742249, 54.033586),
            (-1.225891, 52.792797),
            (-4.40094, 53.389881)
        ]
        events = self.Event.objects(location__geo_within_polygon=polygon2)
        self.assertEqual(events.count(), 0)

    # $minDistance was added in MongoDB v2.6, but continued being buggy
    # until v3.0; skip for older versions
    @needs_mongodb_v3
    def test_2dsphere_near_and_min_max_distance(self):
        """Ensure "min_distace" and "max_distance" operators work well
        together with the "near" operator in a 2dsphere index.
        """
        event1, event2, event3 = self._create_event_data(
            point_field_class=PointField
        )

        # ensure min_distance and max_distance combine well
        events = self.Event.objects(
            location__near=[-87.67892, 41.9120459],
            location__min_distance=1000,
            location__max_distance=10000
        ).order_by("-date")
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event3)

        # ensure ordering is respected by "near" with "min_distance"
        events = self.Event.objects(
            location__near=[-87.67892, 41.9120459],
            location__min_distance=10000
        ).order_by("-date")
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

    def test_2dsphere_geo_within_center(self):
        """Make sure the "geo_within_center" operator works with a
        2dsphere index.
        """
        event1, event2, event3 = self._create_event_data(
            point_field_class=PointField
        )

        # find events within 5 degrees of pitchfork office, chicago
        point_and_distance = [[-87.67892, 41.9120459], 2]
        events = self.Event.objects(
            location__geo_within_center=point_and_distance)
        self.assertEqual(events.count(), 2)
        events = list(events)
        self.assertTrue(event2 not in events)
        self.assertTrue(event1 in events)
        self.assertTrue(event3 in events)

    def _test_embedded(self, point_field_class):
        """Helper test method ensuring given point field class works
        well in an embedded document.
        """
        class Venue(EmbeddedDocument):
            location = point_field_class()
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

    def test_geo_spatial_embedded(self):
        """Make sure GeoPointField works properly in an embedded document."""
        self._test_embedded(point_field_class=GeoPointField)

    def test_2dsphere_point_embedded(self):
        """Make sure PointField works properly in an embedded document."""
        self._test_embedded(point_field_class=PointField)

    # Needs MongoDB > 2.6.4 https://jira.mongodb.org/browse/SERVER-14039
    @needs_mongodb_v3
    def test_spherical_geospatial_operators(self):
        """Ensure that spherical geospatial queries are working."""
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
            location__within_spherical_distance=[
                [-122, 37.5],
                60 / earth_radius
            ]
        )
        self.assertEqual(points.count(), 2)

        points = Point.objects(location__near_sphere=[-122, 37.5],
                               location__max_distance=60 / earth_radius)
        self.assertEqual(points.count(), 2)

        # Test query works with max_distance, being farer from one point
        points = Point.objects(location__near_sphere=[-122, 37.8],
                               location__max_distance=60 / earth_radius)
        close_point = points.first()
        self.assertEqual(points.count(), 1)

        # Test query works with min_distance, being farer from one point
        points = Point.objects(location__near_sphere=[-122, 37.8],
                               location__min_distance=60 / earth_radius)
        self.assertEqual(points.count(), 1)
        far_point = points.first()
        self.assertNotEqual(close_point, far_point)

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
            location__within_spherical_distance=[
                [-122, 36.5],
                60 / earth_radius
            ]
        )
        self.assertEqual(points.count(), 1)
        self.assertEqual(points[0].id, south_point.id)

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

    def test_aspymongo_with_only(self):
        """Ensure as_pymongo works with only"""
        class Place(Document):
            location = PointField()

        Place.drop_collection()
        p = Place(location=[24.946861267089844, 60.16311983618494])
        p.save()
        qs = Place.objects().only('location')
        self.assertDictEqual(
            qs.as_pymongo()[0]['location'],
            {u'type': u'Point',
             u'coordinates': [
                24.946861267089844,
                60.16311983618494]
            }
        )

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
