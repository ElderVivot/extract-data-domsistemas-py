from typing import Dict, Any
from aiohttp import ClientSession


async def get(session: ClientSession, url: str, headers: Dict[str, str]):
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        return data, response.status


async def post(session: ClientSession, url: str, data: Any, headers: Dict[str, str]):
    async with session.post(url, json=data, headers=headers) as response:
        data = await response.json()
        return data, response.status


async def put(session: ClientSession, url: str, data: Any, headers: Dict[str, str]):
    async with session.put(url, data=data, headers=headers) as response:
        data = await response.json()
        return data, response.status


async def patch(session: ClientSession, url: str, data: Dict[str, str], headers: Dict[str, str]):
    async with session.patch(url, data=data, headers=headers) as response:
        data = await response.json()
        return data, response.status
