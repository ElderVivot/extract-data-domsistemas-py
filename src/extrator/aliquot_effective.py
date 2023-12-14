# coding: utf-8

from __future__ import annotations
from logging import Logger
import os
import sys
import pandas as pd
import json
from datetime import date
# import asyncio

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

from connection_db import ConnectionDB
from common.utils.read_sql import readSql
from common.exceptions.fetch_sql import FetchSQLExcepction
from services.send_api_aliquot_effective import SendApiAliquotEffective
from common.exceptions.requests import RequestException


class AliquotEffectiveExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger
        self.__competence = date.today().strftime('%Y-%m-%d')

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__sendToApi = SendApiAliquotEffective()

    async def processAsync(self):
        try:
            sql = readSql(os.path.join(folderSrc, "sqls"), "aliquot_effective.sql", {"date_filter": self.__competence})
            df = pd.read_sql_query(sql, self.__connection)
            resultFetch = json.loads(df.to_json(orient="records", date_format="iso"))

            for result in resultFetch:
                print(result)

            # await self.__sendToApi.main({
            #     "codeCompanieAccountSystem": str(companie['codiEmp']),
            #     "dataList": resultFetch
            # })

            # self.__logger.info(f'Save success aliquot_effective {companie["codiEmp"]} - {companie["nameEmp"]} - {companie["federalRegistration"]} - qtd {len(resultFetch)}')
        except Exception as e:
            raise FetchSQLExcepction(e)
        finally:
            self.__connectionDB.closeConnection()

    # def executeJobAsync(self):
    #     try:
    #         asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    #         asyncio.run(self.__main())
    #     except Exception as e:
    #         print(e)


# if __name__ == "__main__":
#     import logging

#     logger = logging.getLogger()
#     handler = logging.StreamHandler()
#     formatter = logging.Formatter(
#         '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
#     logger.setLevel(logging.DEBUG)

#     main = ExtractData(logger, currentFolder, 'foempregados.sql', {"codi_emp": "3"})
#     print(main.fetchData())
