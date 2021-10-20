import os.path
from  pathlib import Path

import pytest
from asynch.cursors import Cursor, DictCursor

from clickhouse_migrate import __version__
from clickhouse_migrate.migrate import MigrationError, Status, MigrationRecord

MIGRATION_NAME = 'new_test_migration'


def test_version():
    assert __version__ == '0.1.0'


@pytest.mark.asyncio
@pytest.fixture
async def migrations(ch_migration):
    base = Path(ch_migration.migrations_path)
    for num in range(0, 5):
        (base / f'{num+1:0>5d}_{MIGRATION_NAME}.up.sql').write_text(
            f'CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)'
        )
        (base / f'{num+1:0>5d}_{MIGRATION_NAME}.down.sql').write_text(
            f'DROP TABLE Table{num+1};'
        )


@pytest.mark.asyncio
async def test_migrate_make(ch_migration):
    await ch_migration.make(MIGRATION_NAME)
    num = 1
    assert os.path.exists(os.path.join(ch_migration.migrations_path, f'{num:0>5d}_{MIGRATION_NAME}.up.sql'))
    assert os.path.exists(os.path.join(ch_migration.migrations_path, f'{num:0>5d}_{MIGRATION_NAME}.down.sql'))

    with pytest.raises(MigrationError):
        await ch_migration.make(MIGRATION_NAME)

    await ch_migration.make(MIGRATION_NAME, force=True)
    num += 1
    assert os.path.exists(os.path.join(ch_migration.migrations_path, f'{num:0>5d}_{MIGRATION_NAME}.up.sql'))
    assert os.path.exists(os.path.join(ch_migration.migrations_path, f'{num:0>5d}_{MIGRATION_NAME}.down.sql'))


@pytest.mark.asyncio
async def test_migrate_up_down(ch_migration, migrations):
    await ch_migration.up()
    async with ch_migration.connection.cursor(cursor=Cursor) as cursor:
        await cursor.execute(f"SHOW TABLES LIKE '{ch_migration.migrations_table}'")
        res = cursor.fetchall()
    assert len(res) > 0

    for num in range(0, 5):
        async with ch_migration.connection.cursor(cursor=Cursor) as cursor:
            await cursor.execute(f"SHOW TABLES LIKE 'Table{num+1}'")
            res = cursor.fetchall()
        assert len(res) > 0
    await ch_migration.down()
    for num in range(0, 5):
        async with ch_migration.connection.cursor(cursor=Cursor) as cursor:
            await cursor.execute(f"SHOW TABLES LIKE 'Table{num+1}'")
            res = cursor.fetchall()
        assert len(res) == 0


async def get_last_log(ch_migration):
    async with ch_migration.connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute(
            f'SELECT version, name, status, up_md5, down_md5, created_at '
            f'FROM `{ch_migration.migrations_table}` '
            f'ORDER BY (created_at) DESC LIMIT 1'
        )
        res = cursor.fetchone()
        return MigrationRecord(**res)


@pytest.mark.asyncio
async def test_migrate_up_step(ch_migration, migrations):
    await ch_migration.up(step=1)
    mig1 = await get_last_log(ch_migration)
    assert mig1.version == 1

    await ch_migration.up(step=2)
    mig2 = await get_last_log(ch_migration)
    assert mig2.version == 3


@pytest.mark.asyncio
async def test_migrate_down_step(ch_migration, migrations):
    await ch_migration.up()
    mig0 = await get_last_log(ch_migration)
    assert mig0.version == 5

    await ch_migration.down(step=1)
    mig1 = await get_last_log(ch_migration)
    assert mig1.version == 5
    assert mig1.status == Status.DOWN

    await ch_migration.down(step=2)
    mig2 = await get_last_log(ch_migration)
    assert mig2.version == 3
    assert mig1.status == Status.DOWN


@pytest.mark.asyncio
async def test_migrate_force(ch_migration, migrations):
    await ch_migration.up()
    mig1 = await get_last_log(ch_migration)

    await ch_migration._log_action(
        version=mig1.version,
        name=mig1.name,
        status=Status.DIRTY_UP,
        up_md5=mig1.up_md5,
        down_md5=mig1.down_md5,
    )
    mig2 = await get_last_log(ch_migration)
    assert mig1.version == mig2.version
    assert mig2.status == Status.DIRTY_UP

    await ch_migration.force()
    mig3 = await get_last_log(ch_migration)
    assert mig1.version == mig3.version
    assert mig3.status == Status.UP


@pytest.mark.asyncio
async def test_migrate_reset(ch_migration, migrations):
    await ch_migration.up()
    mig1 = await get_last_log(ch_migration)

    await ch_migration._log_action(
        version=mig1.version-1,
        name=mig1.name,
        status=Status.UP,
        up_md5=mig1.up_md5,
        down_md5=mig1.down_md5,
    )
    await ch_migration._log_action(
        version=mig1.version,
        name=mig1.name,
        status=Status.DIRTY_UP,
        up_md5=mig1.up_md5,
        down_md5=mig1.down_md5,
    )
    mig2 = await get_last_log(ch_migration)
    assert mig2.version == mig1.version
    assert mig2.status == Status.DIRTY_UP

    await ch_migration.force(reset=True)
    mig3 = await get_last_log(ch_migration)
    assert mig3.version == mig1.version-1
    assert mig3.status == Status.UP
