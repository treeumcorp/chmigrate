import os
import shutil
import tempfile

import pytest
from asynch.connection import connect, Connection

from clickhouse_migrate.migrate import ClickHouseMigrate

CLICKHOUSE_DSN = os.environ.get('TEST_CLICKHOUSE_DSN', 'clickhouse://localhost:9000/test_migrate')


@pytest.fixture()
async def connection() -> Connection:
    # force create db
    conn = Connection(CLICKHOUSE_DSN)
    db = conn.database
    conn = await connect(host=conn.host, port=conn.port, user=conn.user, password=conn.password)
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(f'DROP DATABASE IF EXISTS {db}')
            await cursor.execute(f'CREATE DATABASE {db}')
    finally:
        await conn.close()

    try:
        conn = await connect(CLICKHOUSE_DSN)
        yield conn  # fixture result
    finally:
        async with conn.cursor() as cursor:
            await cursor.execute(f'DROP DATABASE {conn.database}')
        await conn.close()


@pytest.fixture()
def migrations_path():
    test_dir = tempfile.mkdtemp()
    yield test_dir
    shutil.rmtree(test_dir)


@pytest.fixture()
def ch_migration(connection, migrations_path) -> ClickHouseMigrate:
    return ClickHouseMigrate(connection=connection, migrations_path=migrations_path)
