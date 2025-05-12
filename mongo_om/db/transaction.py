from typing import TYPE_CHECKING

from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode

from mongo_om import sync

if TYPE_CHECKING:
    from .session import Session


class Transaction:

    def __init__(
        self,
        sess: "Session",
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: _ServerMode | None = None,
    ):
        sess._sess.start_transaction(
            read_concern=read_concern or sess.db.read_concern,  # type: ignore
            write_concern=write_concern or sess.db.write_concern,  # type: ignore
            read_preference=read_preference or sess.db.read_preference,  # type: ignore
        )
        self.sess = sess

    async def acommit(self):
        await self.sess._sess.commit_transaction()

    async def aabort(self):
        await self.sess._sess.abort_transaction()

    def commit(self):
        sync.run(self.acommit())

    def abort(self):
        sync.run(self.aabort())

    async def __aenter__(self) -> "Transaction":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            await self.aabort()
        else:
            await self.acommit()

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            self.abort()
        else:
            self.commit()
