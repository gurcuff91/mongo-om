from collections import ChainMap
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, TypedDict

import bson
import pydantic
import pymongo
from bson import CodecOptions
from pydantic._internal import _model_construction
from pymongo import WriteConcern
from pymongo.collation import Collation
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode

from mongo_om import sync
from mongo_om.db.collection import Collection
from mongo_om.db.references import Ref
from mongo_om.db.session import Session
from mongo_om.types import ObjectId

if TYPE_CHECKING:
    from mongo_om.db.database import Database

__all__ = ("OMConfig", "Document")


class OMConfig(TypedDict, total=False):
    db: "Database"
    collection: str
    id_field: str
    indexes: list[pymongo.IndexModel]
    refs: list[Ref]
    collation: Collation | None
    codec_options: CodecOptions | None
    read_preference: _ServerMode | None
    write_concern: WriteConcern | None
    read_concern: ReadConcern | None
    ts_field: str | None
    ts_meta_field: str | None
    ts_granularity: Literal["seconds", "minutes", "hours"]
    ts_expire_after: int
    capped: bool
    capped_size: int
    capped_max_docs: int


class _DocumentMeta(_model_construction.ModelMetaclass):
    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        # build document class
        _cls = super().__new__(mcs, cls_name, bases, namespace, **kwargs)
        if namespace.get("__om_base_document__", False):
            return _cls
        # get om config
        _config: OMConfig = ChainMap(
            namespace.get("om_config", {}),
            _cls.model_config.get("om_config", {}),  # type: ignore
        )
        # check db instance
        _db = _config.get("db")
        if not _db:
            raise ValueError(f"{cls_name}.om_config.db is required")
        # create mongo_om collection
        _coll = _db.Collection(  # type: ignore
            _cls,
            name=_config.get("collection", f"{cls_name}s".lower()),
            id_field=_config.get("id_field", "id"),
            indexes=_config.get("indexes", []),
            refs=_config.get("refs", []),
            collation=_config.get("collation"),
            codec_options=_config.get("codec_options"),
            read_preference=_config.get("read_preference"),
            write_concern=_config.get("write_concern"),
            read_concern=_config.get("read_concern"),
            ts_field=_config.get("ts_field"),
            ts_meta_field=_config.get("ts_meta_field"),
            ts_granularity=_config.get("ts_granularity", "seconds"),
            ts_expire_after=_config.get("ts_expire_after", -1),
            capped=_config.get("capped", False),
            capped_size=_config.get("capped_size", 16 * (2**20)),
            capped_max_docs=_config.get("capped_max_docs", -1),
        )
        # set Document class vars
        setattr(_cls, "om_config", OMConfig(**_config))
        setattr(_cls, "collection", _coll)
        return _cls


class Document(pydantic.BaseModel, metaclass=_DocumentMeta):
    __om_base_document__ = True

    model_config = pydantic.ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
    )

    id: ObjectId = pydantic.Field(default_factory=bson.ObjectId)
    om_config: ClassVar[OMConfig]
    collection: ClassVar[Collection[Self]]

    @classmethod
    async def acreate(cls, data: dict, session: Session | None = None) -> Self:
        doc = cls(**data)
        await doc.asave(session)
        return doc

    async def asave(self, session: Session | None = None):
        await self.collection.asave(self, session=session)  # type: ignore

    async def adelete(self, session: Session | None = None):
        await self.collection.adelete(self, session=session)  # type: ignore

    @classmethod
    def create(cls, data: dict, session: Session | None = None) -> Self:
        return sync.run(cls.acreate(data, session=session))

    def save(self, session: Session | None = None):
        self.collection.save(self, session=session)  # type: ignore

    def delete(self, session: Session | None = None):
        self.collection.delete(self, session=session)  # type: ignore
