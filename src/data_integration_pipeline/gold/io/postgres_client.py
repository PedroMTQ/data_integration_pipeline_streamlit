from datetime import datetime
from typing import Optional, Iterable

import polars as pl
import psycopg
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.common.core.models.templates.base_models import BaseGoldSchemaRecord
from data_integration_pipeline.settings import (
    POSTGRES_CONNECTION_TIMEOUT,
    POSTGRES_CLIENT_BATCH_SIZE,
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.settings import SYNC_LDTS_COLUMN


def _split_schema_statements(schema_sql: str) -> list[str]:
    """Split schema SQL into statements by semicolon, ignoring semicolons inside comments or string literals."""
    statements = []
    current: list[str] = []
    i = 0
    n = len(schema_sql)
    in_single = False  # inside '...' string
    in_double = False  # inside "..." identifier
    in_comment = False  # after -- to EOL
    while i < n:
        c = schema_sql[i]
        if in_comment:
            if c == '\n':
                in_comment = False
                current.append(c)
            i += 1
            continue
        if in_single:
            current.append(c)
            if c == "'":
                if i + 1 < n and schema_sql[i + 1] == "'":
                    current.append("'")
                    i += 1
                else:
                    in_single = False
            i += 1
            continue
        if in_double:
            current.append(c)
            if c == '"':
                if i + 1 < n and schema_sql[i + 1] == '"':
                    current.append('"')
                    i += 1
                else:
                    in_double = False
            i += 1
            continue
        if c == '-' and i + 1 < n and schema_sql[i + 1] == '-':
            in_comment = True
            i += 2
            continue
        if c == "'":
            in_single = True
            current.append(c)
            i += 1
            continue
        if c == '"':
            in_double = True
            current.append(c)
            i += 1
            continue
        if c == ';':
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue
        current.append(c)
        i += 1
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


class PostgresClient:
    def setup_database(self):
        # Use an always-present admin DB to avoid failing when the target DB doesn't exist yet.
        with psycopg.connect(
            user=self.__user,
            password=self.__password,
            host=self.__host,
            port=self.__port,
            dbname='postgres',
            autocommit=True,
            connect_timeout=POSTGRES_CONNECTION_TIMEOUT,
        ) as postgres_connection:
            with postgres_connection.cursor() as cursor:
                database_exists = cursor.execute('SELECT 1 FROM pg_database WHERE datname = %s', (self.__database,)).fetchone()
                if not database_exists:
                    cursor.execute(psycopg.sql.SQL('CREATE DATABASE {}').format(psycopg.sql.Identifier(self.__database)))
        logger.debug(f'Connected successfully to postgres DB:{self.__database} at {self.__host}:{self.__port}')

    def __init__(
        self,
        user: str = POSTGRES_USER,
        password: str = POSTGRES_PASSWORD,
        host: str = POSTGRES_HOST,
        port: str = POSTGRES_PORT,
        database: str = POSTGRES_DATABASE,
        batch_size: int = POSTGRES_CLIENT_BATCH_SIZE,
    ):
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__database = database
        try:
            self.setup_database()
        except Exception as e:
            logger.critical(f'Failed to connect to postgres at <{self.__host}:{self.__port}/{self.__database}> as <{self.__user}>, exception: {e}')
            logger.info(f'Will not setup database {self.__database} at {self.__host}:{self.__port} as {self.__user}')
        try:
            self.db_connection = psycopg.connect(
                user=self.__user,
                password=self.__password,
                host=self.__host,
                port=self.__port,
                dbname=self.__database,
                autocommit=True,
                connect_timeout=POSTGRES_CONNECTION_TIMEOUT,
            )
        except Exception as e:
            logger.critical(f'Failed to connect to database <{self.__database}> at <{self.__host}:{self.__port}> as <{self.__user}>, exception: {e}')
            raise e

    def ping(self) -> None:
        try:
            self.db_connection.execute('SELECT 1')
        except Exception as e:
            logger.critical(f'Failed to ping database <{self.__database}> at <{self.__host}:{self.__port}> as <{self.__user}>, exception: {e}')
            raise e

    def get_database_tables(self):
        cursor = self.db_connection.cursor()
        command = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
        """
        command_results = cursor.execute(command).fetchall()
        cursor.close()
        return [ele[0] for ele in command_results]

    def get_count(self, table_name: str) -> int:
        return self._get_count_rows(table_name)

    def _get_count_rows(self, table_name: str) -> int:
        command = f'SELECT COUNT(1) from "{table_name}"'
        cursor = self.db_connection.cursor()
        res = cursor.execute(command).fetchone()[0]
        cursor.close()
        return res

    def execute(self, sql: str, params: tuple | list | None = None) -> list:
        """Execute a SQL statement and return results."""
        cursor = self.db_connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            if cursor.description:
                return cursor.fetchall()
            return []
        finally:
            cursor.close()

    def read_table(self, table_name: str, columns: Optional[list[str]] = None, limit: Optional[int] = None) -> pl.DataFrame:
        """Read a table into a single Polars DataFrame."""
        if columns:
            col_str = ', '.join(f'"{c}"' for c in columns)
            query = f'SELECT {col_str} FROM "{table_name}"'
        else:
            query = f'SELECT * FROM "{table_name}"'
        if limit:
            query += f' LIMIT {limit}'
        return pl.read_database(query, connection=self.db_connection)

    def execute_schema(self, schema_sql: str) -> None:
        """Execute a full schema script (multi-statement SQL). Runs each statement separately."""
        cursor = self.db_connection.cursor()
        try:
            for stmt in _split_schema_statements(schema_sql):
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    cursor.execute(stmt)
            self.db_connection.commit()
        except Exception as e:
            self.db_connection.rollback()
            logger.error(f'Failed to execute schema: {e}')
            raise
        finally:
            cursor.close()

    def upsert(
        self,
        table_name: str,
        records: list[dict],
        upsert_key: str,
        diff_key: str,
        exclude_from_update: tuple[str],
    ) -> int:
        """
        Perform upsert into PostgreSQL using INSERT ON CONFLICT.

        Args:
            table_name: Target table name
            records: List of dicts to upsert
            upsert_key: Column to use for conflict detection
            exclude_from_update: Columns to exclude from UPDATE clause

        Returns:
            Number of records upserted
        """
        if not records:
            return 0

        columns = list(records[0].keys())
        col_str = ', '.join(f'"{c}"' for c in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        update_cols = [c for c in columns if c not in exclude_from_update]
        update_str = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

        sql = f'''
            INSERT INTO "{table_name}" ({col_str})
            VALUES ({placeholders})
            ON CONFLICT ("{upsert_key}") DO UPDATE SET {update_str}
            WHERE "{table_name}"."{diff_key}" IS DISTINCT FROM EXCLUDED."{diff_key}"
        '''

        params_seq = [tuple(record.get(c) for c in columns) for record in records]
        cursor = self.db_connection.cursor()
        try:
            with self.db_connection.transaction():
                cursor.executemany(sql, params_seq)
            logger.info(f'Upserted {len(records)} records to {table_name}')
            return len(records)
        except Exception as e:
            logger.error(f'Failed to upsert to {table_name}: {e}')
            raise
        finally:
            cursor.close()

    def get_last_commit_timestamp(self, table_name: str) -> Optional[datetime]:
        """Return the UTC datetime of the latest sync, or None if the table doesn't exist or has no rows"""
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(f'SELECT MAX({SYNC_LDTS_COLUMN}) FROM "{table_name}"')
            result = cursor.fetchone()
            if result and result[0]:
                return datetime.fromisoformat(result[0])
        except Exception:
            pass
        finally:
            cursor.close()
        return None

    def get_records_by_hks(self, table_name: str, hks: list[str]) -> Iterable[BaseGoldSchemaRecord]:
        """Fetch full rows from ``table_name`` for the given primary-key list.

        Returns a list of dicts (column_name -> value), one per matched row,
        preserving the order of ``hks``.
        """
        meta_model = ModelMapper.get_data_model(table_name)
        if not meta_model:
            raise ValueError(f'No data model found for {table_name}')
        if not hks:
            return []
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(
                f'SELECT * FROM "{table_name}" WHERE hk = ANY(%(hks)s)',
                {'hks': hks},
            )
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row, strict=True))
                yield meta_model.from_gold_record(row_dict)
        finally:
            cursor.close()
