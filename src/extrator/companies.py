# coding: utf-8

from __future__ import annotations
from logging import Logger
import os
import sys
import pandas as pd
import json
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
from services.send_api_companies import SendApiCompanies
from common.exceptions.requests import RequestException
from common.utils.functions import treatAsNumber


class CompaniesExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__sendApiCompanies = SendApiCompanies()

    async def processAsyn(self):
        try:
            sql = readSql(os.path.join(folderSrc, "sqls"), "companies.sql", {})

            df = pd.read_sql_query(sql, self.__connection)
            df = df.replace({r"\n": "", r"\r": "", r"\t": "", r";": ""}, regex=True)

            companies = json.loads(df.to_json(orient="records", date_format="iso"))

            for companie in companies:
                try:
                    cityRegistration = str(treatAsNumber(companie['cityRegistration']))
                    stateRegistration = str(treatAsNumber(companie['stateRegistration']))
                    companie['cityRegistration'] = '' if cityRegistration == '0' else cityRegistration
                    companie['stateRegistration'] = '' if stateRegistration == '0' else stateRegistration
                    await self.__sendApiCompanies.main(companie)
                    self.__logger.info(f'Save success {companie["codeCompanieAccountSystem"]} - {companie["name"]} - {companie["federalRegistration"]}')
                except RequestException as e:
                    self.__logger.error("RequestException", e)
                except Exception as e:
                    self.__logger.error("Exception", e)
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
