import asyncio
from typing import Coroutine


def run(coro: Coroutine):
    return asyncio.get_event_loop().run_until_complete(coro)
