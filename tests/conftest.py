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
    async with conn.cursor() as cursor:
        await cursor.execute(f'CREATE DATABASE IF NOT EXISTS {r.path.lstrip("/")}')
    yield
    async with conn.cursor() as cursor:
        await cursor.execute(f'DROP DATABASE IF EXISTS {r.path.lstrip("/")}')
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
