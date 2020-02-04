import logging

import aiohttp
from aiohttp.resolver import AsyncResolver

NAMESERVERS = [
    # Google public DNS
    "8.8.8.8",
    "8.8.4.4",
]


async def get_content(url, params):
    resolver = AsyncResolver(nameservers=NAMESERVERS)
    conn = aiohttp.TCPConnector(resolver=resolver)
    async with aiohttp.ClientSession(connector=conn) as session:
        async with await session.get(url, params=params) as response:
            content = await response.read()
            logging.info(
                "Response: %d (%d bytes) from %s", response.status, len(content), url,
            )
            return content
