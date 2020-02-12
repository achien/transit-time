import asyncio
from typing import Awaitable, Callable, List, TypeVar


def chunk(l: List, size: int) -> List[List]:
    chunks = []
    cursor = 0
    while cursor + size < len(l):
        chunks.append(l[cursor : cursor + size])
        cursor += size
    if cursor < len(l):
        chunks.append(l[cursor:])
    return chunks


# Returns unique elements of the list based on some key function
def unique(l: List, key: Callable):
    seen = set()
    output = []
    for elem in l:
        k = key(elem)
        if k not in seen:
            seen.add(k)
            output.append(elem)
    return output


def only(l: List):
    assert len(l) == 1
    return l[0]


T = TypeVar("T")


async def afilter(fun: Callable[[T], Awaitable], l: List[T]) -> List[T]:
    to_keep = await asyncio.gather(*[fun(x) for x in l])
    return [x for (i, x) in enumerate(l) if to_keep[i]]
