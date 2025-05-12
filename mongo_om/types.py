import base64
from typing import Annotated, TypeVar

import bson
from pydantic import BaseModel, BeforeValidator, PlainSerializer, WithJsonSchema
from pydantic.v1.validators import (
    bytes_validator,
    decimal_validator,
    int_validator,
    pattern_validator,
)

T = TypeVar("T", bound=BaseModel)

ObjectId = Annotated[
    bson.ObjectId,
    BeforeValidator(lambda v: bson.ObjectId(v)),
    PlainSerializer(lambda v: str(v), return_type=str, when_used="json-unless-none"),
    WithJsonSchema({"type": "string"}, mode="validation"),
]


Decimal128 = Annotated[
    bson.Decimal128,
    BeforeValidator(lambda v: bson.Decimal128(decimal_validator(v))),
    PlainSerializer(
        lambda v: float(v.to_decimal()), return_type=float, when_used="json-unless-none"
    ),
    WithJsonSchema({"type": "float"}, mode="validation"),
]


Int64 = Annotated[
    bson.Int64,
    BeforeValidator(lambda v: bson.Int64(int_validator(v))),
    PlainSerializer(lambda v: int(v), return_type=int, when_used="json-unless-none"),
    WithJsonSchema({"type": "integer"}, mode="validation"),
]


Binary = Annotated[
    bson.Binary,
    BeforeValidator(lambda v: bson.Binary(bytes_validator(v))),
    PlainSerializer(
        lambda v: base64.b64encode(v).decode(),
        return_type=str,
        when_used="json-unless-none",
    ),
    WithJsonSchema({"type": "string", "format": "byte"}, mode="validation"),
]

Regex = Annotated[
    bson.Regex,
    BeforeValidator(
        lambda v: bson.Regex(pattern_validator(v).pattern, pattern_validator(v).flags)
    ),
    PlainSerializer(
        lambda v: str(v.pattern), return_type=str, when_used="json-unless-none"
    ),
    WithJsonSchema({"type": "string"}, mode="validation"),
]

DatetimeMS = Annotated[
    bson.DatetimeMS,
    BeforeValidator(lambda v: bson.DatetimeMS(v)),
    PlainSerializer(lambda v: int(v), return_type=int, when_used="json-unless-none"),
    WithJsonSchema({"type": "integer"}, mode="validation"),
]

Code = Annotated[
    bson.Code,
    BeforeValidator(lambda v: bson.Code(v)),
    PlainSerializer(lambda v: str(v), return_type=str, when_used="json-unless-none"),
    WithJsonSchema({"type": "string"}, mode="validation"),
]
