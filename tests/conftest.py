import os
import shutil
import tempfile
from urllib.parse import urlparse

import asynch
import pytest

clickhouse_dsn = os.environ.get('TEST_CLICKHOUSE_DSN', 'clickhouse://localhost:9000/test_migrate')


@pytest.fixture(scope='function')
async def database_exist():
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
async def clickhouse_conn(database_exist):
    conn = await asynch.connect(dsn=clickhouse_dsn)
    yield conn
    await conn.close()


@pytest.fixture(scope='function')
def migration_path():
    test_dir = tempfile.mkdtemp()
    yield test_dir
    shutil.rmtree(test_dir)
