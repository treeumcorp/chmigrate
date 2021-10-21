import os.path
import pathlib

import pytest
from asynch.cursors import Cursor, DictCursor

from clickhouse_migrate.migrate import ClickHouseMigrate, MigrationError, Status, MigrationRecord


@pytest.mark.asyncio
async def test_show(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    await m.show()


@pytest.mark.asyncio
async def test_migrate_make(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    assert m._conn == clickhouse_conn
    name = 'new_test_migration'
    await m.make(name)
    num = 1
    assert os.path.exists(os.path.join(migration_path, f'{num:0>5d}_{name}.up.sql'))
    assert os.path.exists(os.path.join(migration_path, f'{num:0>5d}_{name}.down.sql'))
    with pytest.raises(MigrationError):
        await m.make(name)
    await m.make(name, force=True)
    num += 1
    assert os.path.exists(os.path.join(migration_path, f'{num:0>5d}_{name}.up.sql'))
    assert os.path.exists(os.path.join(migration_path, f'{num:0>5d}_{name}.down.sql'))


@pytest.mark.asyncio
async def test_migrate_up_down(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    assert m._conn == clickhouse_conn
    name = 'new_test_migration'
    for num in range(0, 5):
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.up.sql')).write_text(
            f'CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id);'
        )
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.down.sql')).write_text(
            f'DROP TABLE Table{num+1};'
        )
    await m.up()
    async with clickhouse_conn.cursor(cursor=Cursor) as cursor:
        await cursor.execute(f"SHOW TABLES LIKE '{m.migrations_table}'")
        res = cursor.fetchall()
    assert len(res) > 0

    for num in range(0, 5):
        async with clickhouse_conn.cursor(cursor=Cursor) as cursor:
            await cursor.execute(f"SHOW TABLES LIKE 'Table{num+1}'")
            res = cursor.fetchall()
        assert len(res) > 0
    await m.down()
    for num in range(0, 5):
        async with clickhouse_conn.cursor(cursor=Cursor) as cursor:
            await cursor.execute(f"SHOW TABLES LIKE 'Table{num+1}'")
            res = cursor.fetchall()
        assert len(res) == 0


async def get_last_log(m):
    async with m._conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute(
            f"""SELECT version, name, status, up_md5, down_md5, created_at 
                FROM `{m.migrations_table}` ORDER BY (created_at) DESC LIMIT 1"""
        )
        res = cursor.fetchone()
        return MigrationRecord(**res)


@pytest.mark.asyncio
async def test_migrate_up_step(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    assert m._conn == clickhouse_conn
    name = 'new_test_migration'
    for num in range(0, 5):
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.up.sql')).write_text(
            f'CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)'
        )
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.down.sql')).write_text(
            f'DROP TABLE Table{num+1};'
        )
    await m.up(step=1)
    mig1 = await get_last_log(m)
    assert mig1.version == 1

    await m.up(step=2)
    mig2 = await get_last_log(m)
    assert mig2.version == 3


@pytest.mark.asyncio
async def test_migrate_down_step(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    assert m._conn == clickhouse_conn
    name = 'new_test_migration'
    for num in range(0, 5):
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.up.sql')).write_text(
            f'CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)'
        )
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.down.sql')).write_text(
            f'DROP TABLE Table{num+1};'
        )
    await m.up()
    mig0 = await get_last_log(m)
    assert mig0.version == 5

    await m.down(step=1)
    mig1 = await get_last_log(m)
    assert mig1.version == 5
    assert mig1.status == Status.DOWN

    await m.down(step=2)
    mig2 = await get_last_log(m)
    assert mig2.version == 3
    assert mig1.status == Status.DOWN


@pytest.mark.asyncio
async def test_migrate_force(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    assert m._conn == clickhouse_conn
    name = 'new_test_migration'
    for num in range(0, 5):
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.up.sql')).write_text(
            f'CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)'
        )
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.down.sql')).write_text(
            f'DROP TABLE Table{num+1};'
        )
    await m.up()
    mig1 = await get_last_log(m)

    await m._log_action(
        version=mig1.version,
        name=mig1.name,
        status=Status.DIRTY_UP,
        up_md5=mig1.up_md5,
        down_md5=mig1.down_md5,
    )
    mig2 = await get_last_log(m)
    assert mig1.version == mig2.version
    assert mig2.status == Status.DIRTY_UP

    await m.force()
    mig3 = await get_last_log(m)
    assert mig1.version == mig3.version
    assert mig3.status == Status.UP


@pytest.mark.asyncio
async def test_migrate_reset(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    assert m._conn == clickhouse_conn
    name = 'new_test_migration'
    for num in range(0, 5):
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.up.sql')).write_text(
            f'CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)'
        )
        pathlib.Path(os.path.join(migration_path, f'{num+1:0>5d}_{name}.down.sql')).write_text(
            f'DROP TABLE Table{num+1};'
        )
    await m.up()
    mig1 = await get_last_log(m)

    await m._log_action(
        version=mig1.version-1,
        name=mig1.name,
        status=Status.UP,
        up_md5=mig1.up_md5,
        down_md5=mig1.down_md5,
    )
    await m._log_action(
        version=mig1.version,
        name=mig1.name,
        status=Status.DIRTY_UP,
        up_md5=mig1.up_md5,
        down_md5=mig1.down_md5,
    )
    mig2 = await get_last_log(m)
    assert mig2.version == mig1.version
    assert mig2.status == Status.DIRTY_UP

    await m.force(reset=True)
    mig3 = await get_last_log(m)
    assert mig3.version == mig1.version-1
    assert mig3.status == Status.UP


def test_usage_environ_variable(clickhouse_conn, migration_path):
    environ = {
        'TEST_VAR1': 'Table1',
    }
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
        environ=environ,
    )
    result = m._render('CREATE TABLE {TEST_VAR1};')
    assert environ['TEST_VAR1'] in result

