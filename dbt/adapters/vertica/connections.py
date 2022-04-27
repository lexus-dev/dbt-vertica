from typing import List, Optional, Tuple, Any, Iterable, Dict, Union
from contextlib import contextmanager
from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.clients.agate_helper
import dbt.exceptions
import agate
import vertica_python


@dataclass
class verticaCredentials(Credentials):
    host: str
    database: str
    schema: str
    username: str
    password: str
    port: int = 5433
    timeout: int = 3600
    withMaterialization: bool = False


    @property
    def type(self):
        return 'vertica'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        return ('host','port','database','username', 'schema')


class verticaConnectionManager(SQLConnectionManager):
    TYPE = 'vertica'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug(':P Connection is already open')
            return connection

        credentials = connection.credentials

        try:
            conn_info = {
                'host': credentials.host,
                'port': credentials.port,
                'user': credentials.username,
                'password': credentials.password,
                'database': credentials.database,
                'connection_timeout': credentials.timeout,
                'connection_load_balance': True,
                'session_label': f'dbt_{credentials.username}',
            }

            handle = vertica_python.connect(**conn_info)
            connection.state = 'open'
            connection.handle = handle
            logger.debug(f':P Connected to database: {credentials.database} at {credentials.host}')

        except Exception as exc:
            logger.debug(f':P Error connecting to database: {exc}')
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException(str(exc))

        # This is here mainly to support dbt-integration-tests.
        # It globally enables WITH materialization for every connection dbt 
        # makes to Vertica. (Defaults to False)
        # Normal usage would be to use query HINT or declare session parameter in model or hook,
        # but tests do not support hooks and cannot change tests from dbt_utils
        # used in dbt-integration-tests
        if credentials.withMaterialization:
            try:
                logger.debug(f':P Set EnableWithClauseMaterialization')
                cur = connection.handle.cursor()
                cur.execute("ALTER SESSION SET PARAMETER EnableWithClauseMaterialization=1")
                cur.close()

            except Exception as exc:
                logger.debug(f':P Could not EnableWithClauseMaterialization: {exc}')
                pass

        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        return AdapterResponse(
            _message=str(cursor._message),
            rows_affected=cursor.rowcount,
        )

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            while cursor.nextset():
                check = cursor._message
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(
            data,
            column_names
        )

    def execute(self, sql: str, auto_begin: bool = False, fetch: bool = False) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
            try:
                while cursor.nextset():
                    check = cursor._message
                    logger.debug(f'Cursor message is: {check}')
            except Exception as exc:
                logger.debug(f':P Error: {exc}')
                self.release()
                raise dbt.exceptions.DatabaseException(str(exc))
        return response, table

    @classmethod
    def get_status(cls, cursor):
        return str(cursor.rowcount)

    def cancel(self, connection):
        logger.debug(':P Cancel query')
        connection.handle.cancel()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except vertica_python.DatabaseError as exc:
            logger.debug(f':P Database error: {exc}')
            self.release()
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug(f':P Error: {exc}')
            self.release()
            raise dbt.exceptions.RuntimeException(str(exc))


