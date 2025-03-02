import pytest
import os
from dotenv import load_dotenv
from sqlalchemy import Table, Column, Integer, String, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient


@pytest.fixture
def postgres_client():
    load_dotenv()
    client = PostgreSqlClient(
        server_name=os.getenv("SERVER_NAME"),
        database_name=os.getenv("DATABASE_NAME"),
        username=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("PORT"),
    )
    return client


@pytest.fixture
def sample_table():
    name = "sample_table"
    metadata = MetaData()
    table = Table(
        name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("content", String),
    )
    return name, table, metadata


def test_insert_functionality(postgres_client, sample_table):
    client = postgres_client
    table_name, table, metadata = sample_table

    client.drop_table(table_name)

    sample_data = [
        {"id": 1, "content": "super"},
        {"id": 2, "content": "fake"},
        {"id": 3, "content": "data"},
    ]
    client.insert(data=sample_data, table=table, metadata=metadata)

    retrieved_data = client.select_all(table=table)
    assert len(retrieved_data) == 3

    client.drop_table(table_name)
