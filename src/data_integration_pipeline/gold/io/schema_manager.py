import os
import re
from pathlib import Path

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.gold.io.postgres_client import PostgresClient

SCHEMAS_DIR = Path(__file__).parent.parent / 'schemas'

_CREATE_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?',
    re.IGNORECASE,
)


class SchemaManager:
    """Executes SQL schema files against Postgres."""

    def __init__(
        self,
        db_client: PostgresClient,
    ) -> None:
        self.db_client = db_client
        self.schema_folder = SCHEMAS_DIR
        self.schemas: list[str] = []

    def _extract_table_names(self) -> set[str]:
        """Parse loaded schema SQL to extract table names from CREATE TABLE statements."""
        tables: set[str] = set()
        for schema_sql in self.schemas:
            tables.update(_CREATE_TABLE_RE.findall(schema_sql))
        return tables

    def _schema_exists(self) -> bool:
        """Check whether all tables defined in the schema SQL already exist in the database."""
        expected_tables = self._extract_table_names()
        if not expected_tables:
            return False
        existing_tables = set(self.db_client.get_database_tables())
        return expected_tables.issubset(existing_tables)

    def load_schemas(self) -> None:
        """Load schema SQL files from the schemas folder. schema.sql first, then rest sorted."""
        sql_files = [f for f in os.listdir(self.schema_folder) if f.endswith('.sql')]
        # Ensure schema.sql runs first so tables exist before seed scripts
        sql_files.sort(key=lambda f: (0 if f == 'schema.sql' else 1, f))
        self.schemas = []
        for file in sql_files:
            path = os.path.join(self.schema_folder, file)
            with open(path, 'r') as f:
                self.schemas.append(f.read())

    def execute_schemas(self) -> None:
        """Execute each loaded schema SQL against the database."""
        for i, schema_sql in enumerate(self.schemas):
            try:
                self.db_client.execute_schema(schema_sql)
                logger.info(f'Executed schema {i + 1}/{len(self.schemas)}')
            except Exception as e:
                logger.error(f'Failed to execute schema {i + 1}: {e}')
                raise

    def run(self) -> None:
        """Load schema files and execute them against the database."""
        self.load_schemas()
        if self._schema_exists():
            logger.info('Schema already exists, skipping schema execution')
            return
        self.execute_schemas()
