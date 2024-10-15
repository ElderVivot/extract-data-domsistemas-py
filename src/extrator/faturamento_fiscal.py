# coding: utf-8

from __future__ import annotations
from logging import Logger
import os
import sys
import pandas as pd
import json
from datetime import date
import asyncio
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from typing import Dict, Any

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
from services.calcula_aliquota_anexos_simples import CalcularAliquotaAnexosSimples
from common.utils.functions import returnDataInDictOrArray, treatDecimalField, treatDateField


class FaturamentoFiscalExtract:
    def __init__(self, logger: Logger):
        self.__logger = logger
        self.__today = date.today()

        monthsAddCompetenceReferenciaAliquotaEnviarProCliente = 0 if self.__today.day < 25 else 1
        self.__competenceReferenciaAliquotaEnviarProCliente = self.__today + relativedelta(months=monthsAddCompetenceReferenciaAliquotaEnviarProCliente)
        self.__competenceReferenciaStr = self.__competenceReferenciaAliquotaEnviarProCliente.strftime('%Y-%m-01')

        monthsSubtriToCompetenceInicio = 13 if self.__today.day < 25 else 12
        self.__competenceInicio = self.__today - relativedelta(months=monthsSubtriToCompetenceInicio)
        self.__competenceInicioStr = self.__competenceInicio.strftime('%Y-%m-01')

        monthsSubtriToCompetenceFim = 2 if self.__today.day < 25 else 1
        self.__competenceFim = self.__today - relativedelta(months=monthsSubtriToCompetenceFim)
        ultimoDiaMes = monthrange(self.__competenceFim.year, self.__competenceFim.month)[1]
        self.__competenceFimStr = self.__competenceFim.strftime(f'%Y-%m-{ultimoDiaMes}')

        self.__connectionDB = ConnectionDB(self.__logger)
        self.__connection = self.__connectionDB.getConnection()

        self.__calcularAliquotaSimples = CalcularAliquotaAnexosSimples()
        self.__sendToApi = SendApiAliquotEffective()

    def __getFaturamentoFiscal(self, codeCompanie: str):
        sqlFaturamentoFiscal = readSql(os.path.join(folderSrc, "sqls"), "faturamento_fiscal.sql",
                                       {"codi_emp": codeCompanie, "competence": self.__competenceInicioStr, "competence_fim": self.__competenceFimStr}
                                       )
        dfFaturamentoFiscal = pd.read_sql_query(sqlFaturamentoFiscal, self.__connection)
        faturamentoFiscal = json.loads(dfFaturamentoFiscal.to_json(orient="records", date_format="iso"))
        totalRBT12 = treatDecimalField(returnDataInDictOrArray(faturamentoFiscal, [0, 'total_faturamento'], 0))
        return totalRBT12

    def __getSNReceitaFolha(self, codeCompanie: str):
        sqlSNReceitaFolha = readSql(os.path.join(folderSrc, "sqls"), "faturamento_fiscal.sql",
                                    {"codi_emp": codeCompanie, "competence": self.__competenceInicioStr, "competence_fim": self.__competenceFimStr}
                                    )
        dfSNReceitaFolha = pd.read_sql_query(sqlSNReceitaFolha, self.__connection)
        snReceitaFolha = json.loads(dfSNReceitaFolha.to_json(orient="records", date_format="iso"))
        totalReceitaFolha = treatDecimalField(returnDataInDictOrArray(snReceitaFolha, [0, 'valor'], 0))
        return totalReceitaFolha

    def __getAnexosAcumuladorNotas(self, codeCompanie: str):
        sqlAnexosAcumuladorNota = readSql(os.path.join(folderSrc, "sqls"), "anexos_acumulador_notas.sql",
                                          {"codi_emp": codeCompanie, "competence": self.__competenceInicioStr, "competence_fim": self.__competenceFimStr}
                                          )
        dfAnexosAcumuladorNota = pd.read_sql_query(sqlAnexosAcumuladorNota, self.__connection)
        anexosAcumuladorNota = json.loads(dfAnexosAcumuladorNota.to_json(orient="records", date_format="iso"))
        anexos = returnDataInDictOrArray(anexosAcumuladorNota, [0, 'anexos'], '')
        anexo_troca_fator_r = returnDataInDictOrArray(anexosAcumuladorNota, [0, 'anexo_troca_fator_r'], '')
        return anexos, anexo_troca_fator_r

    async def processAsync(self):
        try:
            dataToSave = {}

            sqlGeempre = readSql(os.path.join(folderSrc, "sqls"), "companies_simples_nacional.sql", {"competence": self.__competenceFimStr})
            dfGeempre = pd.read_sql_query(sqlGeempre, self.__connection)
            companies = json.loads(dfGeempre.to_json(orient="records", date_format="iso"))

            for companie in companies:
                try:
                    codeCompanie = companie['codeCompanieAccountSystem']
                    nameCompanie = companie['name']
                    federalRegistration = companie['federalRegistration']
                    dtInicioEmp = companie['dtinicio_emp'][:10]
                    dtInicioEmp = treatDateField(f"{dtInicioEmp[:7]}-01", 2)
                    diffDateInicioEmpAndCompetenceFim = relativedelta(treatDateField(f"{self.__competenceFimStr[:7]}-01", 2), dtInicioEmp)

                    print(codeCompanie, nameCompanie, federalRegistration, self.__competenceInicioStr, self.__competenceFimStr, sep=' | ')

                    qtdMesesEntreAberturaEFaturamento = 12
                    if diffDateInicioEmpAndCompetenceFim.months > 0 and diffDateInicioEmpAndCompetenceFim.years == 0:
                        qtdMesesEntreAberturaEFaturamento = diffDateInicioEmpAndCompetenceFim.months

                    existCompanie = returnDataInDictOrArray(dataToSave, [codeCompanie], None)
                    if existCompanie is None:
                        dataToSave[codeCompanie] = {
                            "codeCompanieAccountSystem": codeCompanie,
                            "competence": self.__competenceReferenciaAliquotaEnviarProCliente.strftime('%Y-%m-01'),
                            "RBT12": 0,
                            "aliquotTotal": 0,
                            "qtdAliquotasCalcular": 0,
                            "aliquot1ICMS": 0,
                            "aliquot2ICMS": 0,
                            "aliquot3ICMS": 0,
                            "aliquot4ICMS": 0,
                            "aliquot5ICMS": 0,
                            "aliquot1ISS": 0,
                            "aliquot2ISS": 0,
                            "aliquot3ISS": 0,
                            "aliquot4ISS": 0,
                            "aliquot5ISS": 0,
                            "aliquot2IPI": 0,
                            "typeLog": 'success',
                            "messageLog": 'SUCCESS',
                            "messageLogToShowUser": "sucesso",
                            "messageError": ''
                        }

                    # se hoje eh 25/05/2024 entao a competencia vai ser 06/2024, o rbt12 vai ser 01/05/2023 a 30/04/2024
                    dataToSave[codeCompanie]['competence'] = self.__competenceReferenciaAliquotaEnviarProCliente.strftime('%Y-%m-01')
                    dataToSave[codeCompanie]['nameEmp'] = nameCompanie

                    rbt12 = self.__getFaturamentoFiscal(codeCompanie)
                    receitaFolha = self.__getSNReceitaFolha(codeCompanie)
                    anexos, anexo_troca_fator_r = self.__getAnexosAcumuladorNotas(codeCompanie)

                    calculoFatorR = receitaFolha / rbt12
                    isFatorR = False
                    if calculoFatorR >= 0.28:
                        isFatorR = True

                    if rbt12 is not None and rbt12 > 0 and anexos is not None:
                        if qtdMesesEntreAberturaEFaturamento < 12:
                            rbt12 = rbt12 / qtdMesesEntreAberturaEFaturamento * 12
                        dataToSave[codeCompanie]["RBT12"] = round(rbt12, 2)

                        anexosSplit = anexos.split(',')
                        for anexo in anexosSplit:
                            result = self.__calcularAliquotaSimples.reparticaoImpostos(rbt12, anexo)
                            aliquota_icms = round(returnDataInDictOrArray(result, ['ICMS'], 0), 2)
                            aliquota_iss = round(returnDataInDictOrArray(result, ['ISS'], 0), 2)
                            aliquota_ipi = round(returnDataInDictOrArray(result, ['IPI'], 0), 2)
                            aliquota_irpj = round(returnDataInDictOrArray(result, ['IRPJ'], 0), 2)
                            aliquota_csll = round(returnDataInDictOrArray(result, ['CSLL'], 0), 2)
                            aliquota_pis = round(returnDataInDictOrArray(result, ['PIS/Pasep'], 0), 2)
                            aliquota_cofins = round(returnDataInDictOrArray(result, ['Cofins'], 0), 2)
                            aliquota_cpp = round(returnDataInDictOrArray(result, ['CPP'], 0), 2)

                            # print(aliquota_icms, aliquota_iss, aliquota_ipi, aliquota_irpj, aliquota_csll, aliquota_pis, aliquota_cofins, aliquota_cpp, sep=' | ')
                            dataToSave[codeCompanie]["aliquotTotal"] += aliquota_icms + aliquota_iss + aliquota_ipi + aliquota_irpj + aliquota_csll + aliquota_pis + aliquota_cofins + aliquota_cpp

                            if aliquota_iss > 5:
                                aliquota_iss = 5

                            if anexo == '1':
                                dataToSave[codeCompanie]["aliquot1ICMS"] = aliquota_icms
                            elif anexo == '2':
                                dataToSave[codeCompanie]["aliquot2ICMS"] = aliquota_icms
                                dataToSave[codeCompanie]["aliquot2IPI"] = aliquota_ipi
                            elif anexo == '3':
                                if anexo_troca_fator_r == 'S' and isFatorR is False:
                                    dataToSave[codeCompanie]["aliquot5ISS"] = aliquota_iss
                                else:
                                    aliquota_iss = 2.01 if aliquota_iss > 0 and aliquota_iss < 2 else aliquota_iss
                                    dataToSave[codeCompanie]["aliquot3ISS"] = aliquota_iss
                            elif anexo == '4':
                                aliquota_iss = 2.00 if aliquota_iss > 0 and aliquota_iss < 2 else aliquota_iss
                                dataToSave[codeCompanie]["aliquot4ISS"] = aliquota_iss
                            elif anexo == '5':
                                if anexo_troca_fator_r == 'S' and isFatorR is True:
                                    aliquota_iss = 2.01 if aliquota_iss > 0 and aliquota_iss < 2 else aliquota_iss
                                    dataToSave[codeCompanie]["aliquot3ISS"] = aliquota_iss
                                else:
                                    dataToSave[codeCompanie]["aliquot5ISS"] = aliquota_iss

                        dataToSave[codeCompanie]["aliquotTotal"] = round(dataToSave[codeCompanie]["aliquotTotal"] / len(anexosSplit), 2)
                        await self.__sendToApi.main(dataToSave[codeCompanie])
                except Exception as e:
                    logger.exception(e)

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
