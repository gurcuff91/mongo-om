from typing import Literal

__all__ = ("Q", "asc", "desc")


class Query(dict[str, dict | list]):

    def __and__(self, other: "Query") -> "Query":
        if not other:
            return self
        if not self:
            return other
        return Query({"$and": [self, other]})

    def __or__(self, other: "Query") -> "Query":  # type: ignore
        if not other:
            return self
        if not self:
            return other
        return Query({"$or": [self, other]})

    def __invert__(self) -> "Query":
        return Query({"$not": self})

    @classmethod
    def _eq(cls, field: str, value) -> "Query":
        return cls({field: {"$eq": value}})

    @classmethod
    def _ne(cls, field: str, value) -> "Query":
        return cls({field: {"$ne": value}})

    @classmethod
    def _gt(cls, field: str, value) -> "Query":
        return cls({field: {"$gt": value}})

    @classmethod
    def _gte(cls, field: str, value) -> "Query":
        return cls({field: {"$gte": value}})

    @classmethod
    def _lt(cls, field: str, value) -> "Query":
        return cls({field: {"$lt": value}})

    @classmethod
    def _lte(cls, field: str, value) -> "Query":
        return cls({field: {"$lte": value}})

    @classmethod
    def _in(cls, field: str, values: list) -> "Query":
        return cls({field: {"$in": values}})

    @classmethod
    def _nin(cls, field: str, values: list) -> "Query":
        return cls({field: {"$nin": values}})

    @classmethod
    def _regex(cls, field: str, value: str) -> "Query":
        return cls({field: {"$regex": value}})


def Q(**kwargs) -> Query:
    q = Query()
    for k, val in kwargs.items():
        tokens = k.split("__")
        field = ".".join(tokens[:-1])
        op = getattr(Query, f"_{tokens[-1]}")
        q = q & op(field, val)
    return q


class Sort(dict[str, Literal[-1] | Literal[1]]):

    def __or__(self, other: "Sort") -> "Sort":  # type: ignore
        if not other:
            return self
        if not self:
            return other
        return Sort({**self, **other})

    @classmethod
    def _asc(cls, field: str) -> "Sort":
        return cls({field: 1})

    @classmethod
    def _desc(cls, field: str) -> "Sort":
        return cls({field: -1})


def asc(*fields: str) -> Sort:
    s = Sort()
    for field in fields:
        s = s | Sort._asc(field)
    return s


def desc(*fields: str) -> Sort:
    s = Sort()
    for field in fields:
        s = s | Sort._desc(field)
    return s
