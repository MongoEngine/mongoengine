from bson import ObjectId

from mongoengine.base import BaseField
from mongoengine.synchronous.connection import DEFAULT_CONNECTION_NAME

from .gridfs_proxy import GridFSProxy


class FileField(BaseField):
    """A GridFS storage field."""

    proxy_class = GridFSProxy

    def __init__(
        self, db_alias=DEFAULT_CONNECTION_NAME, collection_name="fs", **kwargs
    ):
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.db_alias = db_alias

    def __get__(self, instance, owner) -> GridFSProxy:
        if instance is None:
            return self

        # Check if a file already exists for this model
        grid_file = instance._data.get(self.name)
        if not isinstance(grid_file, self.proxy_class):
            grid_file = self.get_proxy_obj(key=self.name, instance=instance)
            instance._data[self.name] = grid_file

        if not grid_file.key:
            grid_file.key = self.name
            grid_file.instance = instance
        return grid_file

    def __set__(self, instance, value):
        key = self.name
        if (
            hasattr(value, "read") and not isinstance(value, GridFSProxy)
        ) or isinstance(value, (bytes, str)):
            # using "FileField() = file/string" notation
            grid_file = instance._data.get(self.name)
            # If a file already exists, delete it
            if grid_file:
                try:
                    grid_file.delete()
                except Exception:
                    pass

            # Create a new proxy object as we don't already have one
            instance._data[key] = self.get_proxy_obj(key=key, instance=instance)
            instance._data[key].put(value)
        else:
            instance._data[key] = value

        instance._mark_as_changed(key)

    def get_proxy_obj(self, key, instance, db_alias=None, collection_name=None):
        if db_alias is None:
            db_alias = self.db_alias
        if collection_name is None:
            collection_name = self.collection_name

        return self.proxy_class(
            key=key,
            instance=instance,
            db_alias=db_alias,
            collection_name=collection_name,
        )

    def to_mongo(self, value):
        # Store the GridFS file id in MongoDB
        if isinstance(value, self.proxy_class) and value.grid_id is not None:
            return value.grid_id
        return None

    def to_python(self, value):
        if value is not None:
            return self.proxy_class(
                value, collection_name=self.collection_name, db_alias=self.db_alias
            )

    def validate(self, value, clean=True):
        if value.grid_id is not None:
            if not isinstance(value, self.proxy_class):
                self.error("FileField only accepts GridFSProxy values")
            if not isinstance(value.grid_id, ObjectId):
                self.error("Invalid GridFSProxy value")


__all__ = ("FileField",)
