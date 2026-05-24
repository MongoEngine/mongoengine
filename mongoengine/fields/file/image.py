import itertools
from io import BytesIO

from mongoengine.session import _get_session
from mongoengine.errors import ValidationError

from .gridfs_proxy import GridFSProxy
from .file_field import FileField
from ..exceptions import ImproperlyConfigured

try:
    from PIL import Image, ImageOps

    if hasattr(Image, "Resampling"):
        LANCZOS = Image.Resampling.LANCZOS
    else:
        LANCZOS = Image.LANCZOS
except ImportError:
    # pillow is optional so may not be installed
    Image = None
    ImageOps = None


class ImageGridFsProxy(GridFSProxy):
    """Proxy for ImageField"""

    def put(self, file_obj, **kwargs):
        """
        Insert a image in database
        applying field properties (size, thumbnail_size)
        """
        field = self.instance._fields[self.key]
        # Handle nested fields
        if hasattr(field, "field") and isinstance(field.field, FileField):
            field = field.field

        try:
            img = Image.open(file_obj)
            img_format = img.format
        except Exception as e:
            raise ValidationError("Invalid image: %s" % e)

        # Progressive JPEG
        # TODO: fixme, at least unused, at worst bad implementation
        progressive = img.info.get("progressive") or False

        if (
            kwargs.get("progressive")
            and isinstance(kwargs.get("progressive"), bool)
            and img_format == "JPEG"
        ):
            progressive = True
        else:
            progressive = False

        if field.size and (
            img.size[0] > field.size["width"] or img.size[1] > field.size["height"]
        ):
            size = field.size

            if size["force"]:
                img = ImageOps.fit(img, (size["width"], size["height"]), LANCZOS)
            else:
                img.thumbnail((size["width"], size["height"]), LANCZOS)

        thumbnail = None
        if field.thumbnail_size:
            size = field.thumbnail_size

            if size["force"]:
                thumbnail = ImageOps.fit(img, (size["width"], size["height"]), LANCZOS)
            else:
                thumbnail = img.copy()
                thumbnail.thumbnail((size["width"], size["height"]), LANCZOS)

        if thumbnail:
            thumb_id = self._put_thumbnail(thumbnail, img_format, progressive)
        else:
            thumb_id = None

        w, h = img.size

        io = BytesIO()
        img.save(io, img_format, progressive=progressive)
        io.seek(0)

        return super().put(
            io, width=w, height=h, format=img_format, thumbnail_id=thumb_id, **kwargs
        )

    async def aput(self, file_obj, **kwargs):
        """
        Insert a image in database
        applying field properties (size, thumbnail_size)
        """
        field = self.instance._fields[self.key]
        # Handle nested fields
        if hasattr(field, "field") and isinstance(field.field, FileField):
            field = field.field

        try:
            img = Image.open(file_obj)
            img_format = img.format
        except Exception as e:
            raise ValidationError("Invalid image: %s" % e)

        # Progressive JPEG
        # TODO: fixme, at least unused, at worst bad implementation
        progressive = img.info.get("progressive") or False

        if (
            kwargs.get("progressive")
            and isinstance(kwargs.get("progressive"), bool)
            and img_format == "JPEG"
        ):
            progressive = True
        else:
            progressive = False

        if field.size and (
            img.size[0] > field.size["width"] or img.size[1] > field.size["height"]
        ):
            size = field.size

            if size["force"]:
                img = ImageOps.fit(img, (size["width"], size["height"]), LANCZOS)
            else:
                img.thumbnail((size["width"], size["height"]), LANCZOS)

        thumbnail = None
        if field.thumbnail_size:
            size = field.thumbnail_size

            if size["force"]:
                thumbnail = ImageOps.fit(img, (size["width"], size["height"]), LANCZOS)
            else:
                thumbnail = img.copy()
                thumbnail.thumbnail((size["width"], size["height"]), LANCZOS)

        if thumbnail:
            thumb_id = await self._aput_thumbnail(thumbnail, img_format, progressive)
        else:
            thumb_id = None

        w, h = img.size

        io = BytesIO()
        img.save(io, img_format, progressive=progressive)
        io.seek(0)

        return await super().aput(
            io, width=w, height=h, format=img_format, thumbnail_id=thumb_id, **kwargs
        )

    def delete(self, *args, **kwargs):
        # deletes thumbnail
        out = self.get()
        if out and out.thumbnail_id:
            self.fs.delete(out.thumbnail_id, session=_get_session())

        return super().delete()

    def _put_thumbnail(self, thumbnail, format, progressive, **kwargs):
        w, h = thumbnail.size

        io = BytesIO()
        thumbnail.save(io, format, progressive=progressive)
        io.seek(0)

        return self.fs.put(io, width=w, height=h, format=format, **kwargs)

    async def _aput_thumbnail(self, thumbnail, format, progressive, **kwargs):
        w, h = thumbnail.size

        io = BytesIO()
        thumbnail.save(io, format, progressive=progressive)
        io.seek(0)

        return await (await self.afs).put(
            io, width=w, height=h, format=format, **kwargs
        )

    @property
    def size(self):
        """
        return a width, height of image
        """
        out = self.get()
        if out:
            return out.width, out.height

    @property
    async def asize(self):
        """
        return a width, height of image
        """
        out = await self.aget()
        if out:
            return out.width, out.height

    @property
    def format(self):
        """
        return format of image
        ex: PNG, JPEG, GIF, etc
        """
        out = self.get()
        if out:
            return out.format

    @property
    async def aformat(self):
        """
        return format of image
        ex: PNG, JPEG, GIF, etc
        """
        out = await self.aget()
        if out:
            return out.format

    @property
    def thumbnail(self):
        """
        return a gridfs.grid_file.GridOut
        representing a thumbnail of Image
        """
        out = self.get()
        if out and out.thumbnail_id:
            return self.fs.get(out.thumbnail_id, session=_get_session())

    @property
    async def athumbnail(self):
        """
        return a gridfs.grid_file.GridOut
        representing a thumbnail of Image
        """
        out = await self.aget()
        if out and out.thumbnail_id:
            return await (await self.afs).get(out.thumbnail_id, session=_get_session())

    def write(self, *args, **kwargs):
        raise RuntimeError('Please use "put" method instead')

    async def awrite(self, *args, **kwargs):
        raise RuntimeError('Please use "aput" method instead')

    def writelines(self, *args, **kwargs):
        raise RuntimeError('Please use "put" method instead')

    async def awritelines(self, *args, **kwargs):
        raise RuntimeError('Please use "aput" method instead')


class ImageField(FileField):
    """
    A Image File storage field.

    :param size: max size to store images, provided as (width, height, force)
        if larger, it will be automatically resized (ex: size=(800, 600, True))
    :param thumbnail_size: size to generate a thumbnail, provided as (width, height, force)
    """

    proxy_class = ImageGridFsProxy

    def __init__(
        self, size=None, thumbnail_size=None, collection_name="images", **kwargs
    ):
        if not Image:
            raise ImproperlyConfigured("PIL library was not found")

        params_size = ("width", "height", "force")
        extra_args = {"size": size, "thumbnail_size": thumbnail_size}
        for att_name, att in extra_args.items():
            value = None
            if isinstance(att, (tuple, list)):
                value = dict(itertools.zip_longest(params_size, att, fillvalue=None))

            setattr(self, att_name, value)

        super().__init__(collection_name=collection_name, **kwargs)


__all__ = ("ImageGridFsProxy", "ImageField")
