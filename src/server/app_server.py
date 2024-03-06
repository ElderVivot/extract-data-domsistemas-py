# coding: utf-8

import sys
import os
import logging
from dotenv import load_dotenv
from rocketry import Rocketry

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

from extrator.companies import CompaniesExtract
from extrator.companies_data import CompaniesDataExtract
from extrator.companies_data_monthly import CompaniesDataMonthlyExtract
from extrator.employee import EmployeeExtract
from extrator.aliquot_effective import AliquotEffectiveExtract

load_dotenv()

SQLS_TO_EXECUTE = str(os.environ.get("SQLS_TO_EXECUTE")).split(",")

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


appRocketry = Rocketry()


@appRocketry.task('every 3 hours', name="companies", execution="async")
async def saveCompanies():
    await CompaniesExtract(logger).processAsyn()


@appRocketry.task('every 3 hours', name="companies_data", execution="async")
async def saveCompaniesData():
    logger.info('Start saveCompaniesData')
    if len(SQLS_TO_EXECUTE) > 0 and SQLS_TO_EXECUTE.count("companies_data") > 0:
        await CompaniesDataExtract(logger).processAsync()


@appRocketry.task('every 8 hours', name="companies_data_monthly", execution="async")
async def saveCompaniesDataMonthly():
    logger.info('Start saveCompaniesDataMonthly')
    if len(SQLS_TO_EXECUTE) > 0 and SQLS_TO_EXECUTE.count("companies_data_monthly") > 0:
        await CompaniesDataMonthlyExtract(logger).processAsync()


@appRocketry.task('daily between 19:00 and 00:00', name="employee", execution="async")
async def saveEmployee():
    logger.info('Start saveEmployee')
    if len(SQLS_TO_EXECUTE) > 0 and SQLS_TO_EXECUTE.count("employee") > 0:
        await EmployeeExtract(logger).processAsync()


@appRocketry.task('daily between 19:00 and 00:00', name="aliquot_effective", execution="async")
async def saveAliquotEffective():
    logger.info('Start saveAliquotEffective')
    if len(SQLS_TO_EXECUTE) > 0 and SQLS_TO_EXECUTE.count("aliquot_effective") > 0:
        await AliquotEffectiveExtract(logger).processAsync()


if __name__ == "__main__":
    appRocketry.run()
