# coding: utf-8

from flask import Flask
from flask_apscheduler import APScheduler
import sys
import os
import logging
from dotenv import load_dotenv

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

from extrator.companies import CompaniesExtract

load_dotenv()

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


scheduler = APScheduler()


class Config:
    """App configuration."""

    SCHEDULER_API_ENABLED = True


def executeWhenStartServer():
    CompaniesExtract(logger).executeJobAsync()


@scheduler.task(trigger="cron", hour="*/3", minute="0", id="esocial_save_database_to_process")
def esocialSaveInDatabaseToProcess():
    CompaniesExtract(logger).executeJobAsync()


if __name__ == "__main__":
    app = Flask(__name__)
    app.config.from_object(Config())

    scheduler.init_app(app)
    scheduler.start()

    executeWhenStartServer()

    app.run()
