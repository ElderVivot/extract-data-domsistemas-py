# coding: utf-8

from __future__ import annotations
from logging import Logger
import sys
import os
from pyodbc import connect, Connection

currentFolder = os.path.dirname(__file__)
folderSrc = os.path.join(currentFolder, "..")
folderBeforeSrc = os.path.join(currentFolder, "..", "..")
sys.path.append(currentFolder)
sys.path.append(folderSrc)
sys.path.append(folderBeforeSrc)

user = os.environ.get("USER_CONNECT_DATABASE")
password = os.environ.get("PASSWORD_CONNECT_DATABASE")
host = os.environ.get("NAME_ODBC_CONNECT_DATABASE")
port = os.environ.get("PORT_CONNECT_DATABASE")


class ConnectionDB:
    def __init__(self, logger: Logger):
        self.__connection: Connection | None = None
        self.__logger = logger

    def getConnection(self) -> Connection | None:
        if self.__connection is None:
            try:
                self.__connection = connect(DSN=host, UID=user, PWD=password, PORT=port)
            except Exception as e:
                self.__logger.error(f"Não foi possível realizar a conexão. O erro é: {e}")
        return self.__connection

    def closeConnection(self):
        if self.__connection is not None:
            try:
                self.__connection.close()
            except Exception as e:
                self.__logger.error(f"Não foi possível fechar a conexão. O erro é: {e}")
