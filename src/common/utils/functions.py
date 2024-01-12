from __future__ import annotations
import re


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


def treatAsNumber(data: str | None):
    return re.sub('[^0-9]', '', data)


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
