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
from services.send_api_employee import SendApiEmployee
from common.exceptions.requests import RequestException


class EmployeeExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__sendToApi = SendApiEmployee()

    async def processAsync(self):
        try:
            sqlGeempre = "SELECT codi_emp AS codiEmp, nome_emp AS nameEmp, cgce_emp AS federalRegistration FROM bethadba.geempre WHERE codi_emp <> 0 ORDER BY 1"
            dfGeempre = pd.read_sql_query(sqlGeempre, self.__connection)
            companies = json.loads(dfGeempre.to_json(orient="records", date_format="iso"))

            for companie in companies:
                try:
                    sql = readSql(os.path.join(folderSrc, "sqls"), "employee.sql", {"codi_emp": str(companie["codiEmp"])})
                    df = pd.read_sql_query(sql, self.__connection)
                    companieData = json.loads(df.to_json(orient="records", date_format="iso"))[0]

                    await self.__sendToApi.main({
                        "codeCompanieAccountSystem": companie['codiEmp'],
                        "dataList": companieData
                    })

                    self.__logger.info(f'Save success employee {companie["codiEmp"]} - {companie["nameEmp"]} - {companie["federalRegistration"]} - qtd {len(companieData)}')
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
