# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]

import copy
import os
import unittest
import tempfile

import gridfs

from nose.plugins.skip import SkipTest
from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.python_support import PY3, b, StringIO

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

TEST_IMAGE_PATH = os.path.join(os.path.dirname(__file__), 'mongoengine.png')
TEST_IMAGE2_PATH = os.path.join(os.path.dirname(__file__), 'mongodb_leaf.png')


class FileTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def tearDown(self):
        self.db.drop_collection('fs.files')
        self.db.drop_collection('fs.chunks')

    def test_file_field_optional(self):
        # Make sure FileField is optional and not required
        class DemoFile(Document):
            the_file = FileField()
        DemoFile.objects.create()

    def test_file_fields(self):
        """Ensure that file fields can be written to and their data retrieved
        """

        class PutFile(Document):
            the_file = FileField()

        PutFile.drop_collection()

        text = b('Hello, World!')
        content_type = 'text/plain'

        putfile = PutFile()
        putfile.the_file.put(text, content_type=content_type, filename="hello")
        putfile.save()

        result = PutFile.objects.first()
        self.assertTrue(putfile == result)
        self.assertEqual("%s" % result.the_file, "<GridFSProxy: hello>")
        self.assertEqual(result.the_file.read(), text)
        self.assertEqual(result.the_file.content_type, content_type)
        result.the_file.delete()  # Remove file from GridFS
        PutFile.objects.delete()

        # Ensure file-like objects are stored
        PutFile.drop_collection()

        putfile = PutFile()
        putstring = StringIO()
        putstring.write(text)
        putstring.seek(0)
        putfile.the_file.put(putstring, content_type=content_type)
        putfile.save()

        result = PutFile.objects.first()
        self.assertTrue(putfile == result)
        self.assertEqual(result.the_file.read(), text)
        self.assertEqual(result.the_file.content_type, content_type)
        result.the_file.delete()

    def test_file_fields_stream(self):
        """Ensure that file fields can be written to and their data retrieved
        """
        class StreamFile(Document):
            the_file = FileField()

        StreamFile.drop_collection()

        text = b('Hello, World!')
        more_text = b('Foo Bar')
        content_type = 'text/plain'

        streamfile = StreamFile()
        streamfile.the_file.new_file(content_type=content_type)
        streamfile.the_file.write(text)
        streamfile.the_file.write(more_text)
        streamfile.the_file.close()
        streamfile.save()

        result = StreamFile.objects.first()
        self.assertTrue(streamfile == result)
        self.assertEqual(result.the_file.read(), text + more_text)
        self.assertEqual(result.the_file.content_type, content_type)
        result.the_file.seek(0)
        self.assertEqual(result.the_file.tell(), 0)
        self.assertEqual(result.the_file.read(len(text)), text)
        self.assertEqual(result.the_file.tell(), len(text))
        self.assertEqual(result.the_file.read(len(more_text)), more_text)
        self.assertEqual(result.the_file.tell(), len(text + more_text))
        result.the_file.delete()

        # Ensure deleted file returns None
        self.assertTrue(result.the_file.read() == None)

    def test_file_fields_set(self):

        class SetFile(Document):
            the_file = FileField()

        text = b('Hello, World!')
        more_text = b('Foo Bar')

        SetFile.drop_collection()

        setfile = SetFile()
        setfile.the_file = text
        setfile.save()

        result = SetFile.objects.first()
        self.assertTrue(setfile == result)
        self.assertEqual(result.the_file.read(), text)

        # Try replacing file with new one
        result.the_file.replace(more_text)
        result.save()

        result = SetFile.objects.first()
        self.assertTrue(setfile == result)
        self.assertEqual(result.the_file.read(), more_text)
        result.the_file.delete()

    def test_file_field_no_default(self):

        class GridDocument(Document):
            the_file = FileField()

        GridDocument.drop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b("Hello World!"))
            f.flush()

            # Test without default
            doc_a = GridDocument()
            doc_a.save()

            doc_b = GridDocument.objects.with_id(doc_a.id)
            doc_b.the_file.replace(f, filename='doc_b')
            doc_b.save()
            self.assertNotEqual(doc_b.the_file.grid_id, None)

            # Test it matches
            doc_c = GridDocument.objects.with_id(doc_b.id)
            self.assertEqual(doc_b.the_file.grid_id, doc_c.the_file.grid_id)

            # Test with default
            doc_d = GridDocument(the_file=b(''))
            doc_d.save()

            doc_e = GridDocument.objects.with_id(doc_d.id)
            self.assertEqual(doc_d.the_file.grid_id, doc_e.the_file.grid_id)

            doc_e.the_file.replace(f, filename='doc_e')
            doc_e.save()

            doc_f = GridDocument.objects.with_id(doc_e.id)
            self.assertEqual(doc_e.the_file.grid_id, doc_f.the_file.grid_id)

        db = GridDocument._get_db()
        grid_fs = gridfs.GridFS(db)
        self.assertEqual(['doc_b', 'doc_e'], grid_fs.list())

    def test_file_uniqueness(self):
        """Ensure that each instance of a FileField is unique
        """
        class TestFile(Document):
            name = StringField()
            the_file = FileField()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        test_file.the_file.put(b('Hello, World!'))
        test_file.save()

        # Second instance
        test_file_dupe = TestFile()
        data = test_file_dupe.the_file.read()  # Should be None

        self.assertTrue(test_file.name != test_file_dupe.name)
        self.assertTrue(test_file.the_file.read() != data)

        TestFile.drop_collection()

    def test_file_saving(self):
        """Ensure you can add meta data to file"""

        class Animal(Document):
            genus = StringField()
            family = StringField()
            photo = FileField()

        Animal.drop_collection()
        marmot = Animal(genus='Marmota', family='Sciuridae')

        marmot_photo = open(TEST_IMAGE_PATH, 'rb')  # Retrieve a photo from disk
        marmot.photo.put(marmot_photo, content_type='image/jpeg', foo='bar')
        marmot.photo.close()
        marmot.save()

        marmot = Animal.objects.get()
        self.assertEqual(marmot.photo.content_type, 'image/jpeg')
        self.assertEqual(marmot.photo.foo, 'bar')

    def test_file_reassigning(self):
        class TestFile(Document):
            the_file = FileField()
        TestFile.drop_collection()

        test_file = TestFile(the_file=open(TEST_IMAGE_PATH, 'rb')).save()
        self.assertEqual(test_file.the_file.get().length, 8313)

        test_file = TestFile.objects.first()
        test_file.the_file = open(TEST_IMAGE2_PATH, 'rb')
        test_file.save()
        self.assertEqual(test_file.the_file.get().length, 4971)

    def test_file_boolean(self):
        """Ensure that a boolean test of a FileField indicates its presence
        """
        class TestFile(Document):
            the_file = FileField()
        TestFile.drop_collection()

        test_file = TestFile()
        self.assertFalse(bool(test_file.the_file))
        test_file.the_file.put(b('Hello, World!'), content_type='text/plain')
        test_file.save()
        self.assertTrue(bool(test_file.the_file))

        test_file = TestFile.objects.first()
        self.assertEqual(test_file.the_file.content_type, "text/plain")

    def test_file_cmp(self):
        """Test comparing against other types"""
        class TestFile(Document):
            the_file = FileField()

        test_file = TestFile()
        self.assertFalse(test_file.the_file in [{"test": 1}])

    def test_image_field(self):
        if not HAS_PIL:
            raise SkipTest('PIL not installed')

        class TestImage(Document):
            image = ImageField()

        TestImage.drop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b("Hello World!"))
            f.flush()

            t = TestImage()
            try:
                t.image.put(f)
                self.fail("Should have raised an invalidation error")
            except ValidationError, e:
                self.assertEqual("%s" % e, "Invalid image: cannot identify image file")

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'rb'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.format, 'PNG')

        w, h = t.image.size
        self.assertEqual(w, 371)
        self.assertEqual(h, 76)

        t.image.delete()

    def test_image_field_reassigning(self):
        if not HAS_PIL:
            raise SkipTest('PIL not installed')

        class TestFile(Document):
            the_file = ImageField()
        TestFile.drop_collection()

        test_file = TestFile(the_file=open(TEST_IMAGE_PATH, 'rb')).save()
        self.assertEqual(test_file.the_file.size, (371, 76))

        test_file = TestFile.objects.first()
        test_file.the_file = open(TEST_IMAGE2_PATH, 'rb')
        test_file.save()
        self.assertEqual(test_file.the_file.size, (45, 101))

    def test_image_field_resize(self):
        if not HAS_PIL:
            raise SkipTest('PIL not installed')

        class TestImage(Document):
            image = ImageField(size=(185, 37))

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'rb'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.format, 'PNG')
        w, h = t.image.size

        self.assertEqual(w, 185)
        self.assertEqual(h, 37)

        t.image.delete()

    def test_image_field_resize_force(self):
        if not HAS_PIL:
            raise SkipTest('PIL not installed')

        class TestImage(Document):
            image = ImageField(size=(185, 37, True))

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'rb'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.format, 'PNG')
        w, h = t.image.size

        self.assertEqual(w, 185)
        self.assertEqual(h, 37)

        t.image.delete()

    def test_image_field_thumbnail(self):
        if not HAS_PIL:
            raise SkipTest('PIL not installed')

        class TestImage(Document):
            image = ImageField(thumbnail_size=(92, 18))

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'rb'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.thumbnail.format, 'PNG')
        self.assertEqual(t.image.thumbnail.width, 92)
        self.assertEqual(t.image.thumbnail.height, 18)

        t.image.delete()

    def test_file_multidb(self):
        register_connection('test_files', 'test_files')

        class TestFile(Document):
            name = StringField()
            the_file = FileField(db_alias="test_files",
                                 collection_name="macumba")

        TestFile.drop_collection()

        # delete old filesystem
        get_db("test_files").macumba.files.drop()
        get_db("test_files").macumba.chunks.drop()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        test_file.the_file.put(b('Hello, World!'),
                          name="hello.txt")
        test_file.save()

        data = get_db("test_files").macumba.files.find_one()
        self.assertEqual(data.get('name'), 'hello.txt')

        test_file = TestFile.objects.first()
        self.assertEqual(test_file.the_file.read(),
                          b('Hello, World!'))

        test_file = TestFile.objects.first()
        test_file.the_file = b('HELLO, WORLD!')
        test_file.save()

        test_file = TestFile.objects.first()
        self.assertEqual(test_file.the_file.read(),
                          b('HELLO, WORLD!'))

    def test_copyable(self):
        class PutFile(Document):
            the_file = FileField()

        PutFile.drop_collection()

        text = b('Hello, World!')
        content_type = 'text/plain'

        putfile = PutFile()
        putfile.the_file.put(text, content_type=content_type)
        putfile.save()

        class TestFile(Document):
            name = StringField()

        self.assertEqual(putfile, copy.copy(putfile))
        self.assertEqual(putfile, copy.deepcopy(putfile))

    def test_get_image_by_grid_id(self):

        if not HAS_PIL:
            raise SkipTest('PIL not installed')

        class TestImage(Document):

            image1 = ImageField()
            image2 = ImageField()

        TestImage.drop_collection()

        t = TestImage()
        t.image1.put(open(TEST_IMAGE_PATH, 'rb'))
        t.image2.put(open(TEST_IMAGE2_PATH, 'rb'))
        t.save()

        test = TestImage.objects.first()
        grid_id = test.image1.grid_id

        self.assertEqual(1, TestImage.objects(Q(image1=grid_id)
                                              or Q(image2=grid_id)).count())

    def test_complex_field_filefield(self):
        """Ensure you can add meta data to file"""

        class Animal(Document):
            genus = StringField()
            family = StringField()
            photos = ListField(FileField())

        Animal.drop_collection()
        marmot = Animal(genus='Marmota', family='Sciuridae')

        marmot_photo = open(TEST_IMAGE_PATH, 'rb')  # Retrieve a photo from disk

        photos_field = marmot._fields['photos'].field
        new_proxy = photos_field.get_proxy_obj('photos', marmot)
        new_proxy.put(marmot_photo, content_type='image/jpeg', foo='bar')
        marmot_photo.close()

        marmot.photos.append(new_proxy)
        marmot.save()

        marmot = Animal.objects.get()
        self.assertEqual(marmot.photos[0].content_type, 'image/jpeg')
        self.assertEqual(marmot.photos[0].foo, 'bar')
        self.assertEqual(marmot.photos[0].get().length, 8313)

if __name__ == '__main__':
    unittest.main()
