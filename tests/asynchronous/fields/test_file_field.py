import copy
import os
import tempfile
from io import BytesIO

import gridfs
import pytest

from mongoengine import *
from mongoengine.asynchronous import async_register_connection, async_get_db
from mongoengine.base.queryset import Q

try:
    from PIL import Image  # noqa: F401

    HAS_PIL = True
except ImportError:
    HAS_PIL = False

from tests.asynchronous.utils import MongoDBAsyncTestCase
from tests.utils import MONGO_TEST_DB


require_pil = pytest.mark.skipif(not HAS_PIL, reason="PIL not installed")

TEST_IMAGE_PATH = os.path.join(os.path.dirname(__file__), "mongoengine.png")
TEST_IMAGE2_PATH = os.path.join(os.path.dirname(__file__), "mongodb_leaf.png")


def get_file(path):
    """Use a BytesIO instead of a file to allow
    to have a one-liner and avoid that the file remains opened"""
    bytes_io = BytesIO()
    with open(path, "rb") as f:
        bytes_io.write(f.read())
    bytes_io.seek(0)
    return bytes_io


class TestFileField(MongoDBAsyncTestCase):
    async def asyncTearDown(self):
        await self.db.drop_collection("fs.files")
        await self.db.drop_collection("fs.chunks")
        await super().asyncTearDown()

    async def test_file_field_optional(self):
        # Make sure FileField is optional and not required
        class DemoFile(Document):
            the_file = FileField()

        await DemoFile.aobjects.create()

    async def test_file_fields(self):
        """Ensure that file fields can be written to and their data retrieved"""

        class PutFile(Document):
            the_file = FileField()

        await PutFile.adrop_collection()

        text = b"Hello, World!"
        content_type = "text/plain"

        putfile = PutFile()
        await putfile.the_file.aput(text, content_type=content_type, filename="hello")
        await putfile.asave()

        result: PutFile = await PutFile.aobjects.first()
        assert putfile == result
        assert (
            await result.the_file.astr()
            == "<GridFSProxy: hello (%s)>" % result.the_file.grid_id
        )
        assert await result.the_file.aread() == text
        the_file = await result.the_file.aget()
        assert the_file.content_type == content_type
        await result.the_file.adelete()  # Remove file from GridFS
        await PutFile.aobjects.delete()

        # Ensure file-like objects are stored
        await PutFile.adrop_collection()

        putfile = PutFile()
        putstring = BytesIO()
        putstring.write(text)
        putstring.seek(0)
        await putfile.the_file.aput(putstring, content_type=content_type)
        await putfile.asave()

        result: PutFile = await PutFile.aobjects.first()
        assert putfile == result
        assert await result.the_file.aread() == text
        the_file = await result.the_file.aget()
        assert the_file.content_type == content_type
        await result.the_file.adelete()

    async def test_file_fields_stream(self):
        """Ensure that file fields can be written to and their data retrieved"""

        class StreamFile(Document):
            the_file = FileField()

        await StreamFile.adrop_collection()

        text = b"Hello, World!"
        more_text = b"Foo Bar"
        content_type = "text/plain"

        streamfile = StreamFile()
        await streamfile.the_file.anew_file(content_type=content_type)
        await streamfile.the_file.awrite(text)
        await streamfile.the_file.awrite(more_text)
        await streamfile.the_file.aclose()
        await streamfile.asave()

        result: StreamFile = await StreamFile.aobjects.first()
        assert streamfile == result
        assert await result.the_file.aread() == text + more_text
        the_file = await result.the_file.aget()
        assert the_file.content_type == content_type
        await the_file.seek(0)
        assert the_file.tell() == 0
        assert await result.the_file.aread(len(text)) == text
        assert the_file.tell() == len(text)
        assert await result.the_file.aread(len(more_text)) == more_text
        assert the_file.tell() == len(text + more_text)
        await result.the_file.adelete()

        # Ensure deleted file returns None
        assert await result.the_file.aread() is None

    async def test_file_fields_stream_after_none(self):
        """Ensure that a file field can be written to after it has been saved as
        None
        """

        class StreamFile(Document):
            the_file = FileField()

        await StreamFile.adrop_collection()

        text = b"Hello, World!"
        more_text = b"Foo Bar"
        content_type = "text/plain"

        streamfile = StreamFile()
        await streamfile.asave()
        await streamfile.the_file.anew_file(content_type=content_type)
        await streamfile.the_file.awrite(text)
        await streamfile.the_file.awrite(more_text)
        await streamfile.the_file.aclose()
        await streamfile.asave()

        result: StreamFile = await StreamFile.aobjects.first()
        assert streamfile == result
        assert await result.the_file.aread() == text + more_text
        the_file = await result.the_file.aget()
        assert the_file.content_type == content_type
        await the_file.seek(0)
        assert the_file.tell() == 0
        assert await result.the_file.aread(len(text)) == text
        assert the_file.tell() == len(text)
        assert await result.the_file.aread(len(more_text)) == more_text
        assert the_file.tell() == len(text + more_text)
        await result.the_file.adelete()

        # Ensure deleted file returns None
        assert await result.the_file.aread() is None

    async def test_file_fields_set(self):
        class SetFile(Document):
            the_file = FileField()

        text = b"Hello, World!"
        more_text = b"Foo Bar"

        await SetFile.adrop_collection()

        setfile = SetFile()
        await setfile.the_file.aput(text)
        await setfile.asave()

        result: SetFile = await SetFile.aobjects.first()
        assert setfile == result
        assert await result.the_file.aread() == text

        # Try replacing a file with a new one
        await result.the_file.areplace(more_text)
        await result.asave()

        result = await SetFile.aobjects.first()
        assert setfile == result
        assert await result.the_file.aread() == more_text
        await result.the_file.adelete()

    async def test_file_field_no_default(self):
        class GridDocument(Document):
            the_file = FileField()

        await GridDocument.adrop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b"Hello World!")
            f.flush()

            # Test without default
            doc_a = GridDocument()
            await doc_a.asave()

            doc_b = await GridDocument.aobjects.with_id(doc_a.id)
            await doc_b.the_file.areplace(f, filename="doc_b")
            await doc_b.asave()
            assert doc_b.the_file.grid_id is not None

            # Test it matches
            doc_c = await GridDocument.aobjects.with_id(doc_b.id)
            assert doc_b.the_file.grid_id == doc_c.the_file.grid_id

            # Test with default
            doc_d = GridDocument()
            await doc_d.the_file.aput(b"")
            await doc_d.asave()

            doc_e = await GridDocument.aobjects.with_id(doc_d.id)
            assert doc_d.the_file.grid_id == doc_e.the_file.grid_id

            await doc_e.the_file.areplace(f, filename="doc_e")
            await doc_e.asave()

            doc_f = await GridDocument.aobjects.with_id(doc_e.id)
            assert doc_e.the_file.grid_id == doc_f.the_file.grid_id

        db = await GridDocument._async_get_db()
        grid_fs = gridfs.AsyncGridFS(db)
        assert ["doc_b", "doc_e"] == await grid_fs.list()

    async def test_file_uniqueness(self):
        """Ensure that each instance of a FileField is unique"""

        class TestFile(Document):
            name = StringField()
            the_file = FileField()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        await test_file.the_file.aput(b"Hello, World!")
        await test_file.asave()

        # Second instance
        test_file_dupe = TestFile()
        data = await test_file_dupe.the_file.aread()  # Should be None

        assert test_file.name != test_file_dupe.name
        assert await test_file.the_file.aread() != data

        await TestFile.adrop_collection()

    async def test_file_saving(self):
        """Ensure you can add meta data to file"""

        class Animal(Document):
            genus = StringField()
            family = StringField()
            photo = FileField()

        await Animal.adrop_collection()
        marmot = Animal(genus="Marmota", family="Sciuridae")

        marmot_photo_content = get_file(TEST_IMAGE_PATH)  # Retrieve a photo from disk
        await marmot.photo.aput(
            marmot_photo_content, content_type="image/jpeg", foo="bar"
        )
        await marmot.photo.aclose()
        await marmot.asave()

        marmot = await Animal.aobjects.get()
        photo = await marmot.photo.aget()
        assert photo.content_type == "image/jpeg"
        assert photo.foo == "bar"

    async def test_file_reassigning(self):
        class TestFile(Document):
            the_file = FileField()

        await TestFile.adrop_collection()

        test_file = TestFile()
        await test_file.the_file.aput(get_file(TEST_IMAGE_PATH))
        test_file: TestFile = await test_file.asave()
        assert (await test_file.the_file.aget()).length == 8313

        test_file: TestFile = await TestFile.aobjects.first()
        await test_file.the_file.areplace(get_file(TEST_IMAGE2_PATH))
        await test_file.asave()
        assert (await test_file.the_file.aget()).length == 4971

    async def test_file_boolean(self):
        """Ensure that a boolean test of a FileField indicates its presence"""

        class TestFile(Document):
            the_file = FileField()

        await TestFile.adrop_collection()

        test_file = TestFile()
        assert not bool(test_file.the_file)
        await test_file.the_file.aput(b"Hello, World!", content_type="text/plain")
        await test_file.asave()
        assert bool(test_file.the_file)

        test_file = await TestFile.aobjects.first()
        assert (await test_file.the_file.aget()).content_type == "text/plain"

    async def test_file_cmp(self):
        """Test comparing against other types"""

        class TestFile(Document):
            the_file = FileField()

        test_file = TestFile()
        assert test_file.the_file not in [{"test": 1}]

    async def test_file_disk_space(self):
        """Test disk space usage when we delete/replace a file"""

        class TestFile(Document):
            the_file = FileField()

        text = b"Hello, World!"
        content_type = "text/plain"

        testfile = TestFile()
        await testfile.the_file.aput(text, content_type=content_type, filename="hello")
        await testfile.asave()

        # Now check fs.files and fs.chunks
        db = await TestFile._async_get_db()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        files_list = await files.to_list(length=None)
        chunks_list = await chunks.to_list(length=None)

        assert len(files_list) == 1
        assert len(chunks_list) == 1

        # Deleting the document should delete the files
        await testfile.adelete()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        files_list = await files.to_list(length=None)
        chunks_list = await chunks.to_list(length=None)

        assert len(files_list) == 0
        assert len(chunks_list) == 0

        # Test case where we don't store a file in the first place
        testfile = TestFile()
        await testfile.asave()
        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        files_list = await files.to_list(length=None)
        chunks_list = await chunks.to_list(length=None)

        assert len(files_list) == 0
        assert len(chunks_list) == 0

        await testfile.adelete()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        files_list = await files.to_list(length=None)
        chunks_list = await chunks.to_list(length=None)

        assert len(files_list) == 0
        assert len(chunks_list) == 0

        # Test case where we overwrite the file
        testfile = TestFile()
        await testfile.the_file.aput(text, content_type=content_type, filename="hello")
        await testfile.asave()

        text = b"Bonjour, World!"
        await testfile.the_file.areplace(
            text, content_type=content_type, filename="hello"
        )
        await testfile.asave()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        files_list = await files.to_list(length=None)
        chunks_list = await chunks.to_list(length=None)

        assert len(files_list) == 1
        assert len(chunks_list) == 1

        await testfile.adelete()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        files_list = await files.to_list(length=None)
        chunks_list = await chunks.to_list(length=None)

        assert len(files_list) == 0
        assert len(chunks_list) == 0

    @require_pil
    async def test_image_field(self):
        class TestImage(Document):
            image = ImageField()

        await TestImage.adrop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b"Hello World!")
            f.flush()

            t = TestImage()
            try:
                await t.image.aput(f)
                self.fail("Should have raised an invalidation error")
            except ValidationError as e:
                assert "%s" % e == "Invalid image: cannot identify image file %s" % f

        t = TestImage()
        await t.image.aput(get_file(TEST_IMAGE_PATH))
        await t.asave()

        t = await TestImage.aobjects.first()

        assert await t.image.aformat == "PNG"

        w, h = await t.image.asize
        assert w == 371
        assert h == 76

        await t.image.adelete()

    @require_pil
    async def test_image_field_reassigning(self):
        class TestFile(Document):
            the_file = ImageField()

        await TestFile.adrop_collection()

        test_file: TestFile = await TestFile().asave()
        await test_file.the_file.aput(get_file(TEST_IMAGE_PATH))
        await test_file.asave()
        assert await test_file.the_file.asize == (371, 76)

        test_file = await TestFile.aobjects.first()
        await test_file.the_file.areplace(get_file(TEST_IMAGE2_PATH))
        await test_file.asave()
        assert await test_file.the_file.asize == (45, 101)

    @require_pil
    async def test_image_field_resize(self):
        class TestImage(Document):
            image = ImageField(size=(185, 37, True))

        await TestImage.adrop_collection()

        t = TestImage()
        await t.image.aput(get_file(TEST_IMAGE_PATH))
        await t.asave()

        t = await TestImage.aobjects.first()

        assert await t.image.aformat == "PNG"
        w, h = await t.image.asize

        assert w == 185
        assert h == 37

        await t.image.adelete()

    @require_pil
    async def test_image_field_resize_force(self):
        class TestImage(Document):
            image = ImageField(size=(185, 37, True))

        await TestImage.adrop_collection()

        t = TestImage()
        await t.image.aput(get_file(TEST_IMAGE_PATH))
        await t.asave()

        t = await TestImage.aobjects.first()

        assert await t.image.aformat == "PNG"
        w, h = await t.image.asize

        assert w == 185
        assert h == 37

        await t.image.adelete()

    @require_pil
    async def test_image_field_thumbnail(self):
        class TestImage(Document):
            image = ImageField(thumbnail_size=(92, 18, True))

        await TestImage.adrop_collection()

        t = TestImage()
        await t.image.aput(get_file(TEST_IMAGE_PATH))
        await t.asave()

        t = await TestImage.aobjects.first()

        assert (await t.image.athumbnail).format == "PNG"
        assert (await t.image.athumbnail).width == 92
        assert (await t.image.athumbnail).height == 18

        await t.image.adelete()

    async def test_file_multidb(self):
        await async_register_connection("test_files", f"{MONGO_TEST_DB}_test_files")

        class TestFile(Document):
            name = StringField()
            the_file = FileField(db_alias="test_files", collection_name="macumba")

        await TestFile.adrop_collection()

        # delete old filesystem
        await (await async_get_db("test_files")).macumba.files.drop()
        await (await async_get_db("test_files")).macumba.chunks.drop()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        await test_file.the_file.aput(b"Hello, World!", name="hello.txt")
        await test_file.asave()

        data = await (await async_get_db("test_files")).macumba.files.find_one()
        assert data.get("name") == "hello.txt"

        test_file = await TestFile.aobjects.first()
        assert await test_file.the_file.aread() == b"Hello, World!"

        test_file = await TestFile.aobjects.first()
        test_file.the_file.aput(b"Hello, World!")
        await test_file.asave()

        test_file = await TestFile.aobjects.first()
        assert await test_file.the_file.aread() == b"Hello, World!"

    async def test_copyable(self):
        class PutFile(Document):
            the_file = FileField()

        await PutFile.adrop_collection()

        text = b"Hello, World!"
        content_type = "text/plain"

        putfile = PutFile()
        await putfile.the_file.aput(text, content_type=content_type)
        await putfile.asave()

        class TestFile(Document):
            name = StringField()

        assert putfile == copy.copy(putfile)
        assert putfile == copy.deepcopy(putfile)

    @require_pil
    async def test_get_image_by_grid_id(self):
        class TestImage(Document):
            image1 = ImageField()
            image2 = ImageField()

        await TestImage.adrop_collection()

        t = TestImage()
        await t.image1.aput(get_file(TEST_IMAGE_PATH))
        await t.image2.aput(get_file(TEST_IMAGE2_PATH))
        await t.asave()

        test = await TestImage.aobjects.first()
        grid_id = test.image1.grid_id

        assert (
            1
            == await TestImage.aobjects(Q(image1=grid_id) or Q(image2=grid_id)).count()
        )

    async def test_complex_field_filefield(self):
        """Ensure you can add meta data to file"""

        class Animal(Document):
            genus = StringField()
            family = StringField()
            photos = ListField(FileField())

        await Animal.adrop_collection()
        marmot = Animal(genus="Marmota", family="Sciuridae")

        with open(TEST_IMAGE_PATH, "rb") as marmot_photo:  # Retrieve a photo from disk
            photos_field = marmot._fields["photos"].field
            new_proxy = photos_field.get_proxy_obj("photos", marmot)
            await new_proxy.aput(marmot_photo, content_type="image/jpeg", foo="bar")

        marmot.photos.append(new_proxy)
        await marmot.asave()

        marmot = await Animal.aobjects.get()
        photo = await marmot.photos[0].aget()
        assert photo.content_type == "image/jpeg"
        assert photo.foo == "bar"
        assert photo.length == 8313
