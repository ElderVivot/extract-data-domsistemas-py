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


class AliquotEffectiveExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger
        self.__competence = date.today() - timedelta(days=32)
        self.__competence = self.__competence.strftime('%Y-%m-01')

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__sendToApi = SendApiAliquotEffective()

    async def processAsync(self):
        try:
            sql = readSql(os.path.join(folderSrc, "sqls"), "aliquot_effective.sql", {"date_filter": self.__competence})
            df = pd.read_sql_query(sql, self.__connection)
            resultFetch = json.loads(df.to_json(orient="records", date_format="iso"))

            dataToSave = {}

            for result in resultFetch:
                codi_emp = result['codi_emp']
                anexo = result['anexo']
                aliquota_icms = result['aliquota_icms']
                aliquota_iss = result['aliquota_iss']
                aliquota_total = result['aliq_tot']

                existCompanie = returnDataInDictOrArray(dataToSave, [codi_emp], None)
                if existCompanie is None:
                    dataToSave[codi_emp] = {}

                dataToSave[codi_emp]['competence'] = self.__competence
                dataToSave[codi_emp]['nameEmp'] = result['nome_emp']

                if aliquota_iss > 5:
                    aliquota_iss = 5

                if anexo == 1:
                    dataToSave[codi_emp]["aliquot1ICMS"] = aliquota_icms
                elif anexo == 2:
                    dataToSave[codi_emp]["aliquot2ICMS"] = aliquota_icms
                elif anexo == 3:
                    aliquota_iss = 2.01 if aliquota_iss > 0 and aliquota_iss < 2 else aliquota_iss
                    dataToSave[codi_emp]["aliquot3ISS"] = aliquota_iss
                elif anexo == 4:
                    aliquota_iss = 2.00 if aliquota_iss > 0 and aliquota_iss < 2 else aliquota_iss
                    dataToSave[codi_emp]["aliquot4ISS"] = aliquota_iss
                elif anexo == 5:
                    dataToSave[codi_emp]["aliquot5ISS"] = aliquota_iss

                dataToSave[codi_emp]["aliquotTotal"] = round(aliquota_total, 2)

            for codiEmp, data in dataToSave.items():
                try:
                    dataSave = {
                        "codeCompanieAccountSystem": str(codiEmp),
                        "competence": data['competence'],
                        "aliquotTotal": data['aliquotTotal'],
                        "aliquot1ICMS": returnDataInDictOrArray(data, ['aliquot1ICMS'], 0),
                        "aliquot2ICMS": returnDataInDictOrArray(data, ['aliquot2ICMS'], 0),
                        "aliquot3ICMS": 0,
                        "aliquot4ICMS": 0,
                        "aliquot5ICMS": 0,
                        "aliquot1ISS": 0,
                        "aliquot2ISS": 0,
                        "aliquot3ISS": returnDataInDictOrArray(data, ['aliquot3ISS'], 0),
                        "aliquot4ISS": returnDataInDictOrArray(data, ['aliquot4ISS'], 0),
                        "aliquot5ISS": returnDataInDictOrArray(data, ['aliquot5ISS'], 0),
                        "typeLog": 'success',
                        "messageLog": 'SUCCESS',
                        "messageLogToShowUser": 'Sucesso ao extrair dados',
                        "messageError": ''
                    }

                    await self.__sendToApi.main(dataSave)
                except Exception as e:
                    print(e)

                self.__logger.info(f'Save success aliquot_effective {dataSave["codeCompanieAccountSystem"]} - {data["nameEmp"]} - {self.__competence}')
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


# if __name__ == "__main__":
#     import logging

#     logger = logging.getLogger()
#     handler = logging.StreamHandler()
#     formatter = logging.Formatter(
#         '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
#     logger.setLevel(logging.DEBUG)

#     main = AliquotEffectiveExtract(logger)
#     print(main.executeJobAsync())
