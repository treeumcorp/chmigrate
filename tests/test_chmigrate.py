import os.path
import pathlib
from urllib.parse import urlparse

import asynch
import pytest
from asynch.cursors import Cursor, DictCursor

from clickhouse_migrate.migrate import (
    ClickHouseMigrate,
    MigrationError,
    Status,
    MigrationRecord,
)


@pytest.mark.asyncio
async def test_without_connection(migration_path):
    with pytest.raises(RuntimeError):
        ClickHouseMigrate(
            migrations_path=migration_path,
        )


@pytest.mark.asyncio
async def test_already_created_connection(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert clickhouse_conn.host == m.connection_params["host"]
    assert clickhouse_conn.port == m.connection_params["port"]
    assert clickhouse_conn.user == m.connection_params["user"]
    assert clickhouse_conn.password == m.connection_params["password"]
    assert clickhouse_conn.database == m.database


@pytest.mark.asyncio
async def test_connect_with_dsn(clickhouse_dsn, clickhouse_conn_info, migration_path):
    m = ClickHouseMigrate(
        clickhouse_dsn=clickhouse_dsn,
        migrations_path=migration_path,
    )
    r = urlparse(clickhouse_dsn)

    assert m.connection_params["host"] == r.hostname
    assert m.connection_params["port"] == r.port
    assert m.connection_params["user"] == r.username
    assert m.connection_params["password"] == r.password
    assert m.database == r.path.strip("/")


@pytest.mark.asyncio
async def test_connect_with_conn_info(clickhouse_conn_info, migration_path):
    m = ClickHouseMigrate(
        host=clickhouse_conn_info.HOST,
        port=clickhouse_conn_info.PORT,
        username=clickhouse_conn_info.USERNAME,
        password=clickhouse_conn_info.PASSWORD,
        database=clickhouse_conn_info.DATABASE,
        migrations_path=migration_path,
    )
    assert m.connection_params["host"] == clickhouse_conn_info.HOST
    assert m.connection_params["port"] == clickhouse_conn_info.PORT
    assert m.connection_params["user"] == clickhouse_conn_info.USERNAME
    assert m.connection_params["password"] == clickhouse_conn_info.PASSWORD
    assert m.database == clickhouse_conn_info.DATABASE


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
    name = "new_test_migration"
    await m.make(name)
    num = 1
    assert os.path.exists(os.path.join(migration_path, f"{num:0>5d}_{name}.up.sql"))
    assert os.path.exists(os.path.join(migration_path, f"{num:0>5d}_{name}.down.sql"))
    await m.make(name, force=True)
    num += 1
    assert os.path.exists(os.path.join(migration_path, f"{num:0>5d}_{name}.up.sql"))
    assert os.path.exists(os.path.join(migration_path, f"{num:0>5d}_{name}.down.sql"))


@pytest.mark.asyncio
async def test_migrate_up_down(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    name = "new_test_migration"
    for num in range(0, 5):
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.up.sql")
        ).write_text(
            f"CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id);"
        )
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.down.sql")
        ).write_text(f"DROP TABLE Table{num+1};")
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


@pytest.fixture()
def get_last_log(clickhouse_dsn):
    async def _fn(m):
        conn = await asynch.connect(dsn=clickhouse_dsn)
        async with conn.cursor(cursor=DictCursor) as cursor:
            await cursor.execute(
                f"""SELECT version, name, status, up_md5, down_md5, created_at 
                    FROM `{m.migrations_table}` ORDER BY (created_at) DESC LIMIT 1"""
            )
            res = cursor.fetchone()
            return MigrationRecord(**res)

    return _fn


@pytest.mark.asyncio
async def test_migrate_up_step(clickhouse_conn, migration_path, get_last_log):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    name = "new_test_migration"
    for num in range(0, 5):
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.up.sql")
        ).write_text(
            f"CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)"
        )
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.down.sql")
        ).write_text(f"DROP TABLE Table{num+1};")
    await m.up(step=1)
    mig1 = await get_last_log(m)
    assert mig1.version == 1

    await m.up(step=2)
    mig2 = await get_last_log(m)
    assert mig2.version == 3


@pytest.mark.asyncio
async def test_migrate_down_step(clickhouse_conn, migration_path, get_last_log):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    name = "new_test_migration"
    for num in range(0, 5):
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.up.sql")
        ).write_text(
            f"CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)"
        )
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.down.sql")
        ).write_text(f"DROP TABLE Table{num+1};")
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
async def test_migrate_force(clickhouse_conn, migration_path, get_last_log):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    name = "new_test_migration"
    for num in range(0, 5):
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.up.sql")
        ).write_text(
            f"CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)"
        )
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.down.sql")
        ).write_text(f"DROP TABLE Table{num+1};")
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
async def test_migrate_reset(clickhouse_conn, migration_path, get_last_log):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    assert isinstance(m, ClickHouseMigrate)
    name = "new_test_migration"
    for num in range(0, 5):
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.up.sql")
        ).write_text(
            f"CREATE TABLE Table{num+1} (id UInt32) Engine=MergeTree ORDER BY (id)"
        )
        pathlib.Path(
            os.path.join(migration_path, f"{num+1:0>5d}_{name}.down.sql")
        ).write_text(f"DROP TABLE Table{num+1};")
    await m.up()
    mig1 = await get_last_log(m)

    await m._log_action(
        version=mig1.version - 1,
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
    assert mig3.version == mig1.version - 1
    assert mig3.status == Status.UP


ddl_replcated = """
        CREATE TABLE Table1 
        {% if CLUSTER_NAME %} 
            ON CLUSTER {{CLUSTER_NAME}}
        {% endif %}
            (x UInt32)
        {% if CLUSTER_NAME %} 
            ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
        {% else %}
            ENGINE = MergeTree
        {% endif %}
        ORDER BY x;
        """


def test_usage_cluster(clickhouse_conn, migration_path):
    environ = {
        "CLUSTER_NAME": "my_cluster",
    }
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
        environ=environ,
    )
    filename = "test_env.up.sql"
    with open(os.path.join(migration_path, filename), "wt") as f:
        f.write(ddl_replcated)
    result = m._render(filename)
    assert environ["CLUSTER_NAME"] in result
    assert "ON CLUSTER" in result
    assert "ReplicatedMergeTree" in result


def test_usage_wo_cluster(clickhouse_conn, migration_path):
    m = ClickHouseMigrate(
        clickhouse_conn=clickhouse_conn,
        migrations_path=migration_path,
    )
    filename = "test_env.up.sql"
    with open(os.path.join(migration_path, filename), "wt") as f:
        f.write(ddl_replcated)
    result = m._render(filename)
    assert "ON CLUSTER" not in result
    assert "ReplicatedMergeTree" not in result
