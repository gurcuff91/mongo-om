from typing import TYPE_CHECKING

from motor.motor_asyncio import AsyncIOMotorClientSession
from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode

from mongo_om import sync
from mongo_om.db.transaction import Transaction
from mongo_om.errors import SessionError

if TYPE_CHECKING:
    from .database import Database


class Session:

    def __init__(self, db: "Database"):
        self.__sess__ = None
        self.db = db

    @property
    def _sess(self) -> AsyncIOMotorClientSession:
        if not self.__sess__:
            raise SessionError("Session not started")
        return self.__sess__

    async def astart(self):
        if self.__sess__:
            return
        self.__sess__ = await self.db._client.start_session()

    def start(self):
        sync.run(self.astart())

    async def aend(self):
        if not self.__sess__:
            return
        await self.__sess__.end_session()
        self.__sess__ = None

    def end(self):
        sync.run(self.aend())

    def transaction(
        self,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: _ServerMode | None = None,
    ) -> Transaction:
        return Transaction(
            self,
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
        )

    async def __aenter__(self) -> "Session":
        await self.astart()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aend()

    def __enter__(self) -> "Session":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()
