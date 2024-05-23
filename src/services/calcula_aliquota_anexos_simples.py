import os
import sys
import json
import pandas as pd

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

from services.reparticoes_sn_anexos import reparticao_anexo1, reparticao_anexo2, reparticao_anexo3, reparticao_anexo4, reparticao_anexo5


class CalcularAliquotaAnexosSimples():
    def __init__(self) -> None:
        self.__tabelasSimples = {}
        self.__carregarTabelasSimples()

    def __carregarTabelasSimples(self):
        with open('tabelas_simples_nacional.json', 'r') as file:
            self.__tabelasSimples = json.load(file)

    def __calcularAliquotaEfetiva(self, valor_rba: float, anexo_numero: str):
        faixas = self.__tabelasSimples.get(f"Anexo{anexo_numero}", [])
        for i, faixa in enumerate(faixas, start=1):
            limite, aliquota, deduzir = faixa.values()
            if valor_rba <= limite:
                aliquota_efetiva = max(0, (valor_rba * (aliquota / 100) - deduzir) / valor_rba)
                return {"aliquota_efetiva": aliquota_efetiva * 100, "faixa": f"{i}ª Faixa"}
        return None

    def reparticaoImpostos(self, valor_rba: float, anexo_numero: str):
        resultCalculoAliquotaEfetiva = self.__calcularAliquotaEfetiva(valor_rba, anexo_numero)

        if resultCalculoAliquotaEfetiva is not None:
            aliquota_efetiva = resultCalculoAliquotaEfetiva["aliquota_efetiva"]
            faixa_utilizada = resultCalculoAliquotaEfetiva["faixa"]

            # Seleciona a estrutura de repartição correta com base no anexo (faz importacao no from import reparticos_sn_anexos)
            reparticao = globals()[f"reparticao_anexo{anexo_numero}"]

            # Encontra os dados de repartição para a faixa correspondente
            reparticao_faixa = next((item for item in reparticao if item["Faixa"] == faixa_utilizada), None)

            # Calcula a repartição dos impostos - retorna um dicionario com o nome dos impostos e o calculo da aliquota efetiva proporcional em cada um
            reparticao_impostos = {imposto: aliquota_efetiva * (percentual / 100)
                                   for imposto, percentual in reparticao_faixa.items() if imposto not in ('Faixa')}
            # aliquota_efetiva_total = 0
            # for percentual in reparticao_impostos.values():
            #     aliquota_efetiva_total += percentual
            return reparticao_impostos


if __name__ == "__main__":
    import logging
    import time

    timeStart = time.time()

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    result = CalcularAliquotaAnexosSimples().reparticaoImpostos(464241.72, 1)
    print(result)

    timeEnd = time.time()
    secondsProcessing = timeEnd - timeStart

    print('Levou', secondsProcessing, 'segundos pra processar')
