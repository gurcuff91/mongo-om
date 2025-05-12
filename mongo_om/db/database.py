import asyncio
from collections import defaultdict
from typing import Literal, Type

import pymongo
from bson import CodecOptions
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import WriteConcern
from pymongo.collation import Collation
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode

from mongo_om import sync
from mongo_om.db.collection import Collection as Coll
from mongo_om.db.references import Ref
from mongo_om.db.session import Session
from mongo_om.errors import DatabaseError
from mongo_om.types import T

__all__ = ("Database",)


class Database:

    def __init__(
        self,
        name: str,
        collation: Collation | None = None,
        codec_options: CodecOptions | None = None,
        read_preference: _ServerMode | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
    ):
        self.__db__ = None
        self.__colls__ = {}  # type: ignore
        self.name = name
        self.collation = collation
        self.codec_options = codec_options
        self.read_preference = read_preference
        self.write_concern = write_concern
        self.read_concern = read_concern

    @property
    def _db(self) -> AsyncIOMotorDatabase:
        if self.__db__ is None:
            raise DatabaseError("Database not connected")
        return self.__db__

    @property
    def _client(self) -> AsyncIOMotorClient:
        return self._db.client

    async def aconnect(self, uri: str = "mongodb://localhost:27017"):
        if self.__db__ is not None:
            return

        # create a new client and ping the server
        client = AsyncIOMotorClient(uri)  # type: ignore
        await client.admin.command("ping")
        # get the database
        self.__db__ = client.get_database(
            self.name,
            codec_options=self.codec_options,
            read_preference=self.read_preference,
            write_concern=self.write_concern,
            read_concern=self.read_concern,
        )  # type: ignore

    def connect(self, uri: str = "mongodb://localhost:27017"):
        sync.run(self.aconnect(uri))

    async def _apply(
        self,
        ops: list[tuple],
        session: Session | None = None,
    ):
        """
        Apply operations into database
        """
        # segment ops by collection
        colls_ops = defaultdict(list)
        for coll, op in ops:
            colls_ops[coll].append(op)
        # apply ops by collection
        coros = []
        for coll, op in colls_ops.items():
            coros.append(self._coll_apply(coll, op, session=session))
        await asyncio.gather(*coros)

    async def _coll_apply(self, coll: Coll, ops: list, session: Session | None = None):
        """
        Apply operations into collection
        """
        c = await coll._db_coll(session)
        await c.bulk_write(
            ops,
            ordered=True,
            bypass_document_validation=True,
            session=session._sess if session else None,
        )

    def session(self) -> Session:
        return Session(self)

    def Collection(
        self,
        model: Type[T],
        name: str | None = None,
        id_field: str = "id",
        indexes: list[pymongo.IndexModel] = [],
        refs: list[Ref] = [],
        collation: Collation | None = None,
        codec_options: CodecOptions | None = None,
        read_preference: _ServerMode | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        ts_field: str | None = None,
        ts_meta_field: str | None = None,
        ts_granularity: Literal["seconds", "minutes", "hours"] = "seconds",
        ts_expire_after: int = -1,
        capped: bool = False,
        capped_size: int = 16 * (2**20),  # 16MB
        capped_max_docs: int = -1,
        **options,
    ) -> Coll[T]:
        coll = Coll(
            self,
            model=model,
            name=name,
            id_field=id_field,
            indexes=indexes,
            refs=refs,
            collation=collation,
            codec_options=codec_options,
            read_preference=read_preference,
            write_concern=write_concern,
            read_concern=read_concern,
            ts_field=ts_field,
            ts_meta_field=ts_meta_field,
            ts_granularity=ts_granularity,
            ts_expire_after=ts_expire_after,
            capped=capped,
            capped_size=capped_size,
            capped_max_docs=capped_max_docs,
            **options,
        )
        self.__colls__[coll.name] = coll
        return coll

    def TimeSeriesCollection(
        self,
        model: Type[T],
        field: str,
        meta_field: str | None,
        granularity: Literal["seconds", "minutes", "hours"] = "seconds",
        expire_after: int = -1,
        **options,
    ) -> Coll[T]:
        return self.Collection(
            model,
            ts_field=field,
            ts_meta_field=meta_field,
            ts_granularity=granularity,
            ts_expire_after=expire_after,
            **options,
        )

    def CappedCollection(
        self,
        model: Type[T],
        size: int,
        max_docs: int = -1,
        **options,
    ) -> Coll[T]:
        return self.Collection(
            model,
            capped=True,
            capped_size=size,
            capped_max_docs=max_docs,
            **options,
        )
