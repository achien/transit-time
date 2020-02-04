import asyncio


async def gatherd(d: dict, return_exceptions=False):
    results = await asyncio.gather(*d.values(), return_exceptions=return_exceptions)
    return dict(zip(d.keys(), results))
