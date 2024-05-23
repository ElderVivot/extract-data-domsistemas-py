from __future__ import annotations
import re
import datetime
from typing import List, Any


def correlationTypeCgce(typeCgce: str):
    typeCgceInt = int(typeCgce)
    if typeCgceInt == 1:
        return "cnpj"
    elif typeCgceInt == 2:
        return "cpf"
    elif typeCgceInt == 3:
        return "cei"
    elif typeCgceInt == 6:
        return "caepf"
    else:
        return "cnpj"


def correlationStatus(status: str):
    if status == "A":
        return "ACTIVE"
    elif status == "I":
        return "INACTIVE"
    else:
        return "ACTIVE"


def correlationTaxRegime(taxRegime: str):
    taxRegimeInt = int(taxRegime)
    if taxRegimeInt == 2 or taxRegimeInt == 4:
        return "01"
    elif taxRegimeInt == 5:
        return "02"
    elif taxRegimeInt == 1:
        return "03"
    else:
        return "99"


def convertToNumber(data: str | None):
    if data is None:
        return 0
    return int(data)


def treatAsNumber(value: str | None, isInt=False):
    if type(value) == int:
        return value
    try:
        value = re.sub("[^0-9]", '', value)
        if value == "" and isInt is True:
            return 0
        else:
            if isInt is True:
                try:
                    return int(value)
                except Exception:
                    return 0
            return value
    except Exception:
        return 0


def convertToString(data: str | None):
    if data is None:
        return ""
    return str(data)


def returnMonthsOfYear(year, filterMonthStart, filterYearStart, filterMonthEnd, filterYearEnd):
    if year == filterYearStart and year == filterYearEnd:
        months = list(range(filterMonthStart, filterMonthEnd + 1))  # o mais 1 é pq o range pega só pega do inicial até o último antes do final, exemplo: range(0,3) = [0,1,2]
    elif year == filterYearStart:
        months = list(range(filterMonthStart, 13))
    elif year == filterYearEnd:
        months = list(range(1, filterMonthEnd + 1))
    else:
        months = list(range(1, 13))

    return months


def returnDataInDictOrArray(data: Any, arrayStructureDataReturn: List[Any], valueDefault='') -> Any:
    """
    :data: vector, matrix ou dict with data -> example: {"name": "Obama", "adress": {"zipCode": "1234567"}}
    :arrayStructureDataReturn: array in order with position of vector/matriz or name property of dict to \
    return -> example: ['adress', 'zipCode'] -> return is '1234567'
    """
    try:
        dataAccumulated = ''
        for i in range(len(arrayStructureDataReturn)):
            if i == 0:
                dataAccumulated = data[arrayStructureDataReturn[i]]
            else:
                dataAccumulated = dataAccumulated[arrayStructureDataReturn[i]]
        return dataAccumulated
    except Exception:
        return valueDefault


def treatDecimalField(value, numberOfDecimalPlaces=2, decimalSeparator=','):
    if type(value) == float:
        return value
    try:
        value = str(value)
        value = re.sub('[^0-9.,-]', '', value)
        if decimalSeparator == '.' and value.find(',') >= 0 and value.find('.') >= 0:
            value = value.replace(',', '')
        elif value.find(',') >= 0 and value.find('.') >= 0:
            value = value.replace('.', '')

        if value.find(',') >= 0:
            value = value.replace(',', '.')

        if value.find('.') < 0:
            value = int(value)

        return float(value)
    except Exception:
        return float(0)


def treatDateField(valorCampo, formatoData=1):
    """
    :param valorCampo: Informar o campo string que será transformado para DATA
    :param formatoData: 1 = 'DD/MM/YYYY' ; 2 = 'YYYY-MM-DD' ; 3 = 'YYYY/MM/DD' ; 4 = 'DDMMYYYY'
    :return: retorna como uma data. Caso não seja uma data válida irá retornar None
    """
    if type(valorCampo) == 'datetime.date' or type(valorCampo) == 'datetime.datetime':
        return valorCampo

    if isinstance(valorCampo, datetime.datetime):
        return valorCampo.date()

    valorCampo = str(valorCampo).strip()

    lengthField = 10  # tamanho padrão da data são 10 caracteres, só muda se não tiver os separados de dia, mês e ano

    if formatoData == 1:
        formatoDataStr = "%d/%m/%Y"
    elif formatoData == 2:
        formatoDataStr = "%Y-%m-%d"
    elif formatoData == 3:
        formatoDataStr = "%Y/%m/%d"
    elif formatoData == 4:
        formatoDataStr = "%d%m%Y"
        lengthField = 8
    elif formatoData == 5:
        formatoDataStr = "%d/%m/%Y"
        valorCampo = valorCampo[0:6] + '20' + valorCampo[6:]

    try:
        return datetime.datetime.strptime(valorCampo[:lengthField], formatoDataStr)
    except ValueError:
        return None
