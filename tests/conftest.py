import os
import shutil
import tempfile
from dataclasses import dataclass
from urllib.parse import urlparse

import asynch
import pytest


@dataclass
class ClickHouseConnectionInfo:
    HOST: str = "localhost"
    PORT: int = 9000
    DATABASE: str = "test_migrate"
    USERNAME: str = "default"
    PASSWORD: str = ""


@pytest.fixture(scope='function')
async def clickhouse_conn_info():
    yield ClickHouseConnectionInfo(
        HOST=os.environ.get('TEST_CLICKHOUSE_HOST', 'localhost'),
        PORT=os.environ.get('TEST_CLICKHOUSE_PORT', 9000),
        DATABASE=os.environ.get('TEST_CLICKHOUSE_DATABASE', 'test_migrate'),
        USERNAME=os.environ.get('TEST_CLICKHOUSE_USERNAME', 'default'),
        PASSWORD=os.environ.get('TEST_CLICKHOUSE_PASSWORD', ''),
    )


@pytest.fixture(scope='function')
async def clickhouse_dsn():
    yield os.environ.get('TEST_CLICKHOUSE_DSN', 'clickhouse://localhost:9000/test_migrate')


@pytest.fixture(scope='function')
async def database_exist(clickhouse_dsn):
    r = urlparse(clickhouse_dsn)
    conn = await asynch.connect(
        host=r.hostname,
        port=r.port,
        user=r.username or 'default',
        password=r.password or '')
    db_name = r.path.lstrip("/")
    assert db_name.startswith('test_')  # test only testing database
    async with conn.cursor() as cursor:
        await cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    yield
    async with conn.cursor() as cursor:
        await cursor.execute(f'DROP DATABASE IF EXISTS {db_name}')
    await conn.close()


@pytest.fixture(scope='function')
async def clickhouse_conn(clickhouse_dsn, database_exist):
    conn = await asynch.connect(dsn=clickhouse_dsn)
    yield conn
    await conn.close()


@pytest.fixture(scope='function')
def migration_path():
    test_dir = tempfile.mkdtemp()
    yield test_dir
    shutil.rmtree(test_dir)
