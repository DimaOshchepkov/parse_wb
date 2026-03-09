from typing import TypeVar, Awaitable, cast

T = TypeVar("T")

def awaitable(value: Awaitable[T] | T) -> Awaitable[T]:
    return cast(Awaitable[T], value)