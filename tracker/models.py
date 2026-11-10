from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat

MAX_INTEGER = 2**53 - 1
MAX_BODY_BYTES = 256 * 1024


class Point(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")]
    step: Annotated[int, Field(strict=True, ge=0, le=MAX_INTEGER)]
    value: Annotated[FiniteFloat, Field(strict=True)]


class Batch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_id: UUID
    points: Annotated[list[Point], Field(min_length=1, max_length=256)]
