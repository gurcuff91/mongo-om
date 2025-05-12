from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .collection import Collection


class OnDelete(Enum):
    CASCADE = 0
    SET_NULL = 1
    # NOTHING = 2


class Ref:

    def __init__(
        self,
        field: str,
        coll: "Collection",
        ref: str = "id",
        local: str | None = None,
        many: bool = False,
        on_delete: OnDelete = OnDelete.CASCADE,
    ):
        self.field = field
        self.coll = coll
        self.ref = ref
        self.local = local or f"{field}_{ref}"
        self.many = many
        self.on_delete = on_delete


class RefMany(Ref):

    def __init__(
        self,
        field: str,
        coll: "Collection",
        ref: str = "id",
        local: str | None = None,
        on_delete: OnDelete = OnDelete.SET_NULL,
    ):
        super().__init__(
            field,
            coll,
            ref,
            local,
            many=True,
            on_delete=on_delete,
        )


def build_dereference_pipeline(refs: list[Ref]) -> list[dict]:
    from .collection import MONGO_ID

    pipeline = []
    for ref in refs:
        foreing_f = (
            MONGO_ID if ref.ref == ref.coll.id_field else ref.coll._db_field(ref.ref)
        )
        pipeline.append(
            {
                "$lookup": {
                    "from": ref.coll.name,
                    "localField": ref.local,
                    "foreignField": foreing_f,
                    "pipeline": [
                        *build_dereference_pipeline(ref.coll.refs),
                        *([{"$limit": 1}] if not ref.many else []),
                    ],
                    "as": ref.field,
                }
            }
        )
        # set null to not-dereferenced values
        if ref.on_delete == OnDelete.SET_NULL:
            pipeline.append(
                {
                    "$addFields": {
                        ref.field: {
                            "$cond": {
                                "if": {"$eq": [f"${ref.field}", []]},
                                "then": [] if ref.many else [None],
                                "else": f"${ref.field}",
                            }
                        }  # type: ignore
                    },
                }
            )
        # unwind reference values
        if not ref.many:
            pipeline.append(
                {
                    "$unwind": {
                        "path": f"${ref.field}",
                        "preserveNullAndEmptyArrays": True,  # type: ignore
                    }
                }  # type: ignore
            )
        # map MONGO_ID to id field
        pipeline.append({"$set": {ref.coll.id_field: f"${MONGO_ID}"}})  # type: ignore
    return pipeline


def get_reverse_references(coll: "Collection") -> list[tuple]:
    # get all sibling colls (colls in same coll's db)
    colls = (c for c in coll.db.__colls__.values() if c is not coll)
    # get colls that refers to coll
    rev_refs = []
    for c in colls:
        for ref in c.refs:
            if ref.coll is coll:
                rev_refs.append((c, ref))
    return rev_refs
