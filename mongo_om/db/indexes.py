from typing import Callable

import pymongo


def _asc(field: str) -> tuple:
    return field, pymongo.ASCENDING


def _desc(field: str) -> tuple:
    return field, pymongo.DESCENDING


def _text(field: str) -> tuple:
    return field, pymongo.TEXT


def _hashed(field: str) -> tuple:
    return field, pymongo.HASHED


def _geo2d(field: str) -> tuple:
    return field, pymongo.GEO2D


def _geosphere(field: str) -> tuple:
    return field, pymongo.GEOSPHERE


def Index(
    fields: list[str] | str,
    direction: Callable[[str], tuple] = _asc,
    unique: bool = False,
    sparse: bool = False,
    filter: dict = {},
    ttl: int = -1,
    **options,
) -> pymongo.IndexModel:
    if isinstance(fields, str):
        fields = [fields]
    if unique:
        options["unique"] = unique
    if sparse:
        options["sparse"] = sparse
    if filter:
        options["partialFilterExpression"] = filter
    if ttl > 0:
        options["expireAfterSeconds"] = ttl
    return pymongo.IndexModel(keys=[direction(i) for i in fields], **options)


def Descending(
    fields: list[str] | str,
    unique: bool = False,
    sparse: bool = False,
    filter: dict = {},
    ttl: int = -1,
    **options,
) -> pymongo.IndexModel:
    return Index(
        fields,
        direction=_desc,
        unique=unique,
        sparse=sparse,
        filter=filter,
        ttl=ttl,
        **options,
    )


def Text(
    fields: list[str] | str,
    unique: bool = False,
    sparse: bool = False,
    filter: dict = {},
    ttl: int = -1,
    **options,
) -> pymongo.IndexModel:
    return Index(
        fields,
        direction=_text,
        unique=unique,
        sparse=sparse,
        filter=filter,
        ttl=ttl,
        **options,
    )


def Hashed(
    fields: list[str] | str,
    unique: bool = False,
    sparse: bool = False,
    filter: dict = {},
    ttl: int = -1,
    **options,
) -> pymongo.IndexModel:
    return Index(
        fields,
        direction=_hashed,
        unique=unique,
        sparse=sparse,
        filter=filter,
        ttl=ttl,
        **options,
    )


def Geo2d(
    fields: list[str] | str,
    unique: bool = False,
    sparse: bool = False,
    filter: dict = {},
    ttl: int = -1,
    **options,
) -> pymongo.IndexModel:
    return Index(
        fields,
        direction=_geo2d,
        unique=unique,
        sparse=sparse,
        filter=filter,
        ttl=ttl,
        **options,
    )


def Geosphere(
    fields: list[str] | str,
    unique: bool = False,
    sparse: bool = False,
    filter: dict = {},
    ttl: int = -1,
    **options,
) -> pymongo.IndexModel:
    return Index(
        fields,
        direction=_geosphere,
        unique=unique,
        sparse=sparse,
        filter=filter,
        ttl=ttl,
        **options,
    )
