# coding: utf-8

from __future__ import annotations
from logging import Logger
import os
import sys
import pandas as pd
import json
import asyncio
from datetime import datetime
from datetime import timedelta

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

from connection_db import ConnectionDB
from common.utils.read_sql import readSql
from common.exceptions.fetch_sql import FetchSQLExcepction
from services.send_api_companies_data_monthly import SendApiCompaniesDataMonthly
from common.exceptions.requests import RequestException
from common.utils.functions import returnMonthsOfYear


class CompaniesDataMonthlyExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__sendToApi = SendApiCompaniesDataMonthly()

        self.__today = datetime.today()
        self.__competenceStart = self.__today - timedelta(days=180)
        self.__competenceEnd = self.__today

    async def __main(self):
        try:
            sqlGeempre = "SELECT codi_emp AS codiEmp, nome_emp AS nameEmp, cgce_emp AS federalRegistration FROM bethadba.geempre WHERE codi_emp <> 0 ORDER BY 1"
            dfGeempre = pd.read_sql_query(sqlGeempre, self.__connection)
            companies = json.loads(dfGeempre.to_json(orient="records", date_format="iso"))

            for companie in companies:
                year = self.__competenceStart.year
                startYear = self.__competenceStart.year
                startMonth = self.__competenceStart.month
                endYear = self.__competenceEnd.year
                endMonth = self.__competenceEnd.month

                while year <= endYear:
                    months = returnMonthsOfYear(year, startMonth, startYear, endMonth, endYear)

                    for month in months:
                        try:
                            sql = readSql(os.path.join(folderSrc, "sqls"), "companies_data_monthly.sql", {"codi_emp": str(companie["codiEmp"]), "competence": f"{year}-{month:0>2}-01"})
                            df = pd.read_sql_query(sql, self.__connection)
                            companieData = json.loads(df.to_json(orient="records", date_format="iso"))[0]

                            await self.__sendToApi.main(companieData)

                            self.__logger.info(f'Save success companies_data_monthly {year}-{month:0>2} - {companie["codiEmp"]} - {companie["nameEmp"]} - {companie["federalRegistration"]}')
                        except RequestException as e:
                            self.__logger.error("RequestException", e)
                        except Exception as e:
                            self.__logger.error("Exception", e)

                    year += 1
        except Exception as e:
            raise FetchSQLExcepction(e)
        finally:
            self.__connectionDB.closeConnection()

    def executeJobAsync(self):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            asyncio.run(self.__main())
        except Exception as e:
            print(e)


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
