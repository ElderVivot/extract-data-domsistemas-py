from __future__ import annotations


def correlationTypeCgce(typeCgce: str):
    if typeCgce == "1":
        return "cnpj"
    elif typeCgce == "2":
        return "cpf"
    elif typeCgce == "3":
        return "cei"
    elif typeCgce == "6":
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
    if taxRegime == "2" or taxRegime == "4":
        return "01"
    elif taxRegime == "5":
        return "02"
    elif taxRegime == "1":
        return "03"
    else:
        return "99"


def convertToNumber(data: str | None):
    if data is None:
        return 0
    return int(data)


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
