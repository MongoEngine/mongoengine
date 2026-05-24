import gridfs
from gridfs import GridOut, AsyncGridOut
from pymongo.asynchronous.collection import AsyncCollection

from mongoengine.synchronous.connection import DEFAULT_CONNECTION_NAME, get_db
from mongoengine.session import _get_session
from mongoengine.asynchronous import async_get_db
from mongoengine.errors import OperationError

from ..exceptions import GridFSError


class GridFSProxy:
    """Proxy object to handle writing and reading of files to and from GridFS"""

    _fs = None
    _afs = None

    def __init__(
        self,
        grid_id=None,
        key=None,
        instance=None,
        db_alias=DEFAULT_CONNECTION_NAME,
        collection_name="fs",
        _async=False,
    ):
        self.grid_id = grid_id  # Store GridFS id for file
        self.key = key
        self.instance = instance
        self.db_alias = db_alias
        self.collection_name = collection_name
        self.newfile = None  # Used for partial writes
        self.gridout_sync = None
        self.gridout_async = None

    def __getattr__(self, name):
        attrs = (
            "_fs",
            "_afs",
            "grid_id",
            "key",
            "instance",
            "db_alias",
            "collection_name",
            "newfile",
            "gridout",
        )
        if name in attrs:
            return self.__getattribute__(name)
        obj = self.get()
        if hasattr(obj, name):
            return getattr(obj, name)
        raise AttributeError

    def __get__(self, instance, value):
        return self

    def __bool__(self):
        return bool(self.grid_id)

    def __getstate__(self):
        self_dict = self.__dict__
        self_dict["_fs"] = None
        return self_dict

    def __copy__(self):
        copied = GridFSProxy()
        copied.__dict__.update(self.__getstate__())
        return copied

    def __deepcopy__(self, memo):
        return self.__copy__()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.grid_id}>"

    async def astr(self):
        gridout = await self.aget()
        filename = gridout.filename if gridout else "<no file>"
        return f"<{self.__class__.__name__}: {filename} ({self.grid_id})>"

    def __str__(self):
        if isinstance(self.instance._collection, AsyncCollection):
            raise OperationError("use astr()")
        gridout = self.get()
        filename = gridout.filename if gridout else "<no file>"
        return f"<{self.__class__.__name__}: {filename} ({self.grid_id})>"

    def __eq__(self, other):
        if isinstance(other, GridFSProxy):
            return (
                (self.grid_id == other.grid_id)
                and (self.collection_name == other.collection_name)
                and (self.db_alias == other.db_alias)
            )
        else:
            return False

    def __ne__(self, other):
        return not self == other

    @property
    def fs(self):
        if not self._fs:
            self._fs = gridfs.GridFS(get_db(self.db_alias), self.collection_name)
        return self._fs

    @property
    async def afs(self) -> gridfs.AsyncGridFS:
        if not self._afs:
            self._afs = gridfs.AsyncGridFS(
                await async_get_db(self.db_alias), self.collection_name
            )
        return self._afs

    def get(self, grid_id=None) -> GridOut | None:
        if grid_id:
            self.grid_id = grid_id

        if self.grid_id is None:
            return None
        try:
            if self.gridout_sync is None:
                self.gridout_sync = self.fs.get(self.grid_id, session=_get_session())
            return self.gridout_sync
        except Exception:
            # File has been deleted
            return None

    async def aget(self, grid_id=None) -> AsyncGridOut | None:
        if grid_id:
            self.grid_id = grid_id

        if self.grid_id is None:
            return None
        try:
            if self.gridout_async is None:
                self.gridout_async = await (await self.afs).get(
                    self.grid_id, session=_get_session()
                )
            return self.gridout_async
        except Exception:
            # File has been deleted
            return None

    def new_file(self, **kwargs):
        self.newfile = self.fs.new_file(**kwargs)
        self.grid_id = self.newfile._id
        self._mark_as_changed()

    async def anew_file(self, **kwargs):
        self.newfile = (await self.afs).new_file(**kwargs)
        self.grid_id = self.newfile._id
        self._mark_as_changed()

    def put(self, file_obj, **kwargs):
        if isinstance(self.instance._collection, AsyncCollection):
            raise OperationError("use aput()")
        if self.grid_id:
            raise GridFSError(
                "This document already has a file. Either delete "
                "it or call replace to overwrite it"
            )
        self.grid_id = self.fs.put(file_obj, **kwargs)
        self._mark_as_changed()

    async def aput(self, file_obj, **kwargs):
        if self.grid_id:
            raise GridFSError(
                "This document already has a file. Either delete "
                "it or call replace to overwrite it"
            )
        self.grid_id = await (await self.afs).put(file_obj, **kwargs)
        self._mark_as_changed()

    def write(self, string):
        if self.grid_id:
            if not self.newfile:
                raise GridFSError(
                    "This document already has a file. Either "
                    "delete it or call replace to overwrite it"
                )
        else:
            self.new_file()
        self.newfile.write(string)

    async def awrite(self, string):
        if self.grid_id:
            if not self.newfile:
                raise GridFSError(
                    "This document already has a file. Either "
                    "delete it or call replace to overwrite it"
                )
        else:
            await self.anew_file()
        await self.newfile.write(string)

    def writelines(self, lines):
        if not self.newfile:
            self.new_file()
            self.grid_id = self.newfile._id
        self.newfile.writelines(lines)

    async def awritelines(self, lines):
        if not self.newfile:
            await self.anew_file()
            self.grid_id = self.newfile._id
        await self.newfile.writelines(lines)

    def read(self, size=-1):
        gridout = self.get()
        if gridout is None:
            return None
        else:
            try:
                return gridout.read(size)
            except Exception:
                return ""

    async def aread(self, size=-1):
        gridout = await self.aget()
        if gridout is None:
            return None
        else:
            try:
                return await gridout.read(size)
            except Exception:
                return ""

    def delete(self):
        # Delete file from GridFS, FileField still remains
        self.fs.delete(self.grid_id, session=_get_session())
        self.grid_id = None
        self.gridout_sync = None
        self._mark_as_changed()

    async def adelete(self):
        # Delete file from GridFS, FileField still remains
        await (await self.afs).delete(self.grid_id, session=_get_session())
        self.grid_id = None
        self.gridout_async = None
        self._mark_as_changed()

    def replace(self, file_obj, **kwargs):
        if isinstance(self.instance._collection, AsyncCollection):
            raise OperationError("use areplace()")
        self.delete()
        self.put(file_obj, **kwargs)

    async def areplace(self, file_obj, **kwargs):
        await self.adelete()
        await self.aput(file_obj, **kwargs)

    def close(self):
        if self.newfile:
            self.newfile.close()

    async def aclose(self):
        if self.newfile:
            await self.newfile.close()

    def _mark_as_changed(self):
        """Inform the instance that `self.key` has been changed"""
        if self.instance:
            self.instance._mark_as_changed(self.key)


__all__ = ("GridFSProxy",)
