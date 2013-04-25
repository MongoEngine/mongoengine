 # -*- coding: utf-8 -*-
import unittest
import decimal
from decimal import Decimal

from mongoengine import Document, connect
from mongoengine.connection import get_db
from mongoengine.fields import StringField, DecimalField, ListField

__all__ = ('ConvertDecimalField', )


class ConvertDecimalField(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def test_how_to_convert_decimal_fields(self):
        """Demonstrates migrating from 0.7 to 0.8
        """

        # 1. Old definition - using dbrefs
        class Person(Document):
            name = StringField()
            money = DecimalField(force_string=True)
            monies = ListField(DecimalField(force_string=True))

        Person.drop_collection()
        Person(name="Wilson Jr", money=Decimal("2.50"),
               monies=[Decimal("2.10"), Decimal("5.00")]).save()

        # 2. Start the migration by changing the schema
        # Change DecimalField - add precision and rounding settings
        class Person(Document):
            name = StringField()
            money = DecimalField(precision=2, rounding=decimal.ROUND_HALF_UP)
            monies = ListField(DecimalField(precision=2,
                                            rounding=decimal.ROUND_HALF_UP))

        # 3. Loop all the objects and mark parent as changed
        for p in Person.objects:
            p._mark_as_changed('money')
            p._mark_as_changed('monies')
            p.save()

        # 4. Confirmation of the fix!
        wilson = Person.objects(name="Wilson Jr").as_pymongo()[0]
        self.assertTrue(isinstance(wilson['money'], float))
        self.assertTrue(all([isinstance(m, float) for m in wilson['monies']]))
