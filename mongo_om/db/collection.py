from typing import TYPE_CHECKING, Generic, Literal, Type

import bson
import pymongo
from bson import CodecOptions
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import DeleteOne, ReplaceOne, WriteConcern
from pymongo.collation import Collation
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode

from mongo_om import sync
from mongo_om.db.cursor import Cursor
from mongo_om.db.references import (
    OnDelete,
    Ref,
    build_dereference_pipeline,
    get_reverse_references,
)
from mongo_om.db.session import Session
from mongo_om.types import T

if TYPE_CHECKING:
    from .database import Database

MONGO_ID = "_id"


class Collection(Generic[T]):

    def __init__(
        self,
        db: "Database",
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
    ):
        self.__coll__ = None
        self.db = db
        self.model = model
        self.name = name or f"{model.__name__.lower()}s"
        self.id_field = id_field
        self.indexes = indexes
        self.refs = refs
        self.collation = collation or db.collation
        self.codec_options = codec_options or db.codec_options
        self.read_preference = read_preference or db.read_preference
        self.write_concern = write_concern or db.write_concern
        self.read_concern = read_concern or db.read_concern
        self.ts_field = ts_field
        self.ts_meta_field = ts_meta_field
        self.ts_granularity = ts_granularity
        self.ts_expire_after = ts_expire_after
        self.capped = capped
        self.capped_size = capped_size
        self.capped_max_docs = capped_max_docs
        self._options = options

    async def _db_coll(self, session: Session | None = None) -> AsyncIOMotorCollection:
        if self.__coll__ is not None:
            return self.__coll__

        # Check if the collection exists
        colls = await self.db._db.list_collection_names()
        if self.name in colls:
            coll = self.db._db.get_collection(
                self.name,
                codec_options=self.codec_options,
                read_preference=self.read_preference,
                write_concern=self.write_concern,
                read_concern=self.read_concern,
            )
        # If it doesn't exist, create it
        else:
            options = {**self._options}
            options["check_exists"] = False
            # collation options
            if self.collation:
                options["collation"] = self.collation
            # timeseries options
            if self.ts_field:
                options["timeseries"] = {
                    "timeField": self._db_field(self.ts_field),
                    "granularity": self.ts_granularity,
                }
                if self.ts_meta_field:
                    options["timeseries"]["metaField"] = self._db_field(
                        self.ts_meta_field
                    )
                if self.ts_expire_after > 0:
                    options["expireAfterSeconds"] = self.ts_expire_after
            # capped options
            if self.capped:
                options["capped"] = True
                options["size"] = self.capped_size
                if self.capped_max_docs > 0:
                    options["max"] = self.capped_max_docs
            # create collection
            coll = await self.db._db.create_collection(
                self.name,
                codec_options=self.codec_options,
                read_preference=self.read_preference,
                write_concern=self.write_concern,
                read_concern=self.read_concern,
                session=session._sess if session else None,
                **options,
            )
            # create indexes
            if self.indexes:
                await coll.create_indexes(
                    self.indexes,
                    session=session._sess if session else None,
                )
        self.__coll__ = coll  # type: ignore
        return coll

    def _db_field(self, field) -> str:
        return self.model.model_fields[field].alias or field

    def _db_id_field(self) -> str:
        return self._db_field(self.id_field)

    def _db_parse_data(self, data: dict) -> T:
        return self.model.model_validate(data, by_alias=True)

    def _db_dump_data(self, data: T) -> bson.SON:
        son = data.model_dump(by_alias=True)
        # map id field to MONGO_ID
        son[MONGO_ID] = son.pop(self._db_id_field())
        # map references to local field
        for ref in self.refs:
            val = son.pop(ref.field)
            if ref.many:
                val = (
                    [d[ref.coll._db_field(ref.ref)] for d in val]
                    if val is not None
                    else val
                )
            else:
                val = val[ref.coll._db_field(ref.ref)] if val is not None else val
            son[ref.local] = val
        return bson.SON(son)

    def _db_save_op(self, data: list[T]) -> list[tuple]:
        ops = []
        for d in data:
            # references save operations
            for ref in self.refs:
                ref_d = getattr(d, ref.field)
                ref_d = [ref_d] if not ref.many else ref_d
                ref_d = [i for i in ref_d if i is not None]  # not null refs
                ops.extend(ref.coll._db_save_op(ref_d))
            # save operation
            ops.append(
                (
                    self,
                    ReplaceOne(
                        {MONGO_ID: getattr(d, self.id_field)},
                        replacement=self._db_dump_data(d),
                        collation=self.collation,
                        upsert=True,
                    ),
                )
            )
        return ops

    async def _db_delete_op(
        self, data: list[T], session: Session | None = None
    ) -> list[tuple]:
        ops = []
        for d in data:
            # reverse-references delete opterations
            for coll, ref in get_reverse_references(self):
                coll_d = await coll.afetch(
                    {ref.local: getattr(d, ref.ref)},
                    session=session,
                )
                # drop whole data
                if ref.on_delete == OnDelete.CASCADE:
                    ops.extend(await coll._db_delete_op(coll_d))
                # set ref fied to null
                elif ref.on_delete == OnDelete.SET_NULL:
                    for i in coll_d:
                        ref_f = getattr(i, ref.field)
                        # on many-refs, just remove it from list
                        if ref.many:
                            ref_f = [
                                j
                                for j in ref_f
                                if getattr(j, coll.id_field)
                                != getattr(d, coll.id_field)
                            ]
                        # on one-ref, set to null
                        else:
                            ref_f = None
                        setattr(i, ref.field, ref_f)
                    ops.extend(coll._db_save_op(coll_d))
            # delete operation
            ops.append(
                (
                    self,
                    DeleteOne(
                        {MONGO_ID: getattr(d, self.id_field)},
                        collation=self.collation,
                    ),
                )  # type: ignore
            )
        return ops

    def aggregate(
        self,
        pipeline: list[dict],
        session: Session | None = None,
        **options,
    ) -> Cursor[dict]:  # type: ignore
        return Cursor(
            self,
            pipeline=pipeline,
            session=session,
            parse_db_data=False,
            **options,
        )  # type: ignore

    def fetch(
        self,
        filter: dict = {},
        sort: dict = {},
        skip: int = 0,
        limit: int = -1,
        session: Session | None = None,
        cursor_options: dict = {},
    ) -> Cursor[T]:
        pipeline = build_dereference_pipeline(self.refs)
        if filter:
            pipeline.append({"$match": filter})
        if sort:
            pipeline.append({"$sort": sort})
        if skip > 0:
            pipeline.append({"$skip": skip})  # type: ignore
        if limit > 0:
            pipeline.append({"$limit": limit})  # type: ignore
        return Cursor(self, pipeline=pipeline, session=session, **cursor_options)

    async def afetch_one(
        self,
        filter: dict = {},
        sort: dict = {},
        session: Session | None = None,
        cursor_options: dict = {},
    ) -> T | None:
        data = await self.fetch(
            filter,
            sort=sort,
            limit=1,
            session=session,
            cursor_options=cursor_options,
        ).alist()
        if data:
            return data[0]
        return None

    async def asave(self, data: T | list[T], session: Session | None = None):
        data = [data] if not isinstance(data, list) else data
        ops = self._db_save_op(data)
        await self.db._apply(ops, session=session)

    async def adelete(self, data: T | list[T], session: Session | None = None):
        data = [data] if not isinstance(data, list) else data
        ops = await self._db_delete_op(data)
        await self.db._apply(ops, session=session)

    def fetch_one(
        self,
        filter: dict = {},
        sort: dict = {},
        session: Session | None = None,
        cursor_options: dict = {},
    ) -> T | None:
        return sync.run(
            self.afetch_one(
                filter,
                sort=sort,
                session=session,
                cursor_options=cursor_options,
            )
        )

    def save(self, data: T | list[T], session: Session | None = None):
        sync.run(self.asave(data, session=session))

    def delete(self, data: T | list[T], session: Session | None = None):
        sync.run(self.adelete(data, session=session))
