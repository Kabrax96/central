from loguru import logger
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql

class PostgreSqlClient:
    """
    A client for querying PostgreSQL database.
    """

    def __init__(self, server_name: str, database_name: str, username: str, password: str, port: int = 5432):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)
        logger.info(f"200 | PostgreSQL connection established to {database_name}@{server_name}")

    def select_all(self, table: Table) -> list[dict]:
        logger.info(f"100 | Selecting all data from table: {table.name}")
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def create_table(self, metadata: MetaData) -> None:
        logger.info("100 | Creating table in PostgreSQL")
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        logger.warning(f"400 | Dropping table if exists: {table_name}")
        self.engine.execute(f"DROP TABLE IF EXISTS {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        logger.info(f"100 | Inserting data into table: {table.name}")
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        logger.info(f"100 | Upserting data into table: {table.name}")
        metadata.create_all(self.engine)
        key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns},
        )
        self.engine.execute(upsert_statement)
        logger.success(f"200 | Upsert operation completed for table: {table.name}")
