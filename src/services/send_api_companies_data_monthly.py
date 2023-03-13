from __future__ import annotations
import os
import sys
import json
from typing import Any, Dict
from aiohttp import ClientSession

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

API_HOST = os.environ.get("API_HOST")
TENANT = os.environ.get("TENANT")

from common.exceptions.requests import RequestException
from common.adapters.requests import post


class SendApiCompaniesDataMonthly:
    def __init__(self) -> None:
        self.__data: Dict[str, Any] = {}

    def __mountData(self) -> Dict[str, Any]:
        return self.__data

    async def __putData(self):
        async with ClientSession() as session:
            response, statusCode = await post(
                session,
                f"{API_HOST}/companies_data_monthly",
                data=json.loads(json.dumps(self.__data)),
                headers={"tenant": TENANT},
            )
            if statusCode >= 400:
                raise RequestException(statusCode, response)
            return response

    async def main(self, data):
        self.__data.clear()
        self.__data = data
        self.__data = self.__mountData()
        await self.__putData()
