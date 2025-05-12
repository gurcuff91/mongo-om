from typing import TYPE_CHECKING, Generic

from mongo_om import sync
from mongo_om.db.session import Session
from mongo_om.types import T

if TYPE_CHECKING:
    from .collection import Collection


class Cursor(Generic[T]):

    def __init__(
        self,
        collection: "Collection[T]",
        pipeline: list[dict],
        session: Session | None = None,
        parse_db_data: bool = True,
        **options,
    ):
        self.__cursor__ = None
        self.coll = collection
        self._pipeline = pipeline
        self._session = session
        self._parse_db_data = parse_db_data
        self._options = options

    async def __init_db_cursor__(self):
        coll = await self.coll._db_coll(self._session)
        self.__cursor__ = coll.aggregate(
            self._pipeline,
            session=self._session._sess if self._session else None,
            **self._options,
        )  # type: ignore

    async def alist(self) -> list[T]:
        return [i async for i in self]

    def list(self) -> list[T]:
        return sync.run(self.alist())

    def __aiter__(self):
        return self

    def __iter__(self):
        return self

    async def __anext__(self):
        if self.__cursor__ is None:
            await self.__init_db_cursor__()
        data = await anext(self.__cursor__)
        if self._parse_db_data:
            data = self.coll._db_parse_data(data)
        return data

    def __next__(self):
        return sync.run(self.__anext__())
