# coding: utf-8

from __future__ import annotations
from logging import Logger
import os
import sys
import pandas as pd
import json
from datetime import date
import asyncio

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
from datetime import timedelta
from common.utils.functions import returnDataInDictOrArray


class FaturamentoFiscalExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger
        self.__competence = date.today() - timedelta(days=32)
        self.__competence = self.__competence.strftime('%Y-%m-01')

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__sendToApi = SendApiAliquotEffective()

    async def processAsync(self):
        try:
            sqlGeempre = readSql(os.path.join(folderSrc, "sqls"), "companies_simples_nacional.sql", {"competence": self.__competence})
            dfGeempre = pd.read_sql_query(sqlGeempre, self.__connection)
            companies = json.loads(dfGeempre.to_json(orient="records", date_format="iso"))

            for companie in companies:
                print(companie)

        except Exception as e:
            raise FetchSQLExcepction(e)
        finally:
            self.__connectionDB.closeConnection()

    def executeJobAsync(self):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            asyncio.run(self.processAsync())
        except Exception as e:
            print(e)


if __name__ == "__main__":
    import logging

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    main = FaturamentoFiscalExtract(logger)
    print(main.executeJobAsync())
