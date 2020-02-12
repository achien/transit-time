import pytest

import c


@pytest.mark.asyncio
async def test_afilter():
    async def even(x):
        return x % 2 == 0

    res = await c.afilter(even, list(range(5)))
    assert res == [0, 2, 4]
