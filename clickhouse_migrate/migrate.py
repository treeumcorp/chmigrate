import datetime
import hashlib
import os
import pathlib
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

import asynch
from asynch.connection import Connection
from asynch.cursors import DictCursor

from clickhouse_migrate.utils import remove_suffix


class MigrationError(Exception):
    message = 'migration error'

    def __init__(self, message: str = None, *args, **kwargs):
        self.message = message or self.message

    def __str__(self) -> str:
        return self.message


def show_migration_error(fn):
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except MigrationError as e:
            print(e)
    return wrapped


class StrEnum(str, Enum):
    def __str__(self):
        return self.value


class Status(StrEnum):
    PENDING = 'pending'
    DIRTY_UP = 'dirty_up'
    UP = 'up'
    DIRTY_DOWN = 'dirty_down'
    DOWN = 'down'


class Action(StrEnum):
    UP = 'up'
    DOWN = 'down'


@dataclass
class MigrationFiles:
    version: int
    name: str
    up: Optional[str] = None
    up_md5: Optional[str] = None
    down: Optional[str] = None
    down_md5: Optional[str] = None


@dataclass
class MigrationRecord:
    version: int
    name: str = ''
    status: Status = Status.PENDING
    created_at: Optional[datetime.datetime] = None
    up_md5: Optional[str] = None
    down_md5: Optional[str] = None


class ClickHouseMigrate:
    clickhouse_dsn: str
    migrations_table: str = 'schema_migrations'
    migration_path: str = 'migrations'

    environ: Dict[str, Any] = {}

    _conn = None

    MULTISTATE_SEPARATOR = ';'

    def __init__(self, *,
                 clickhouse_dsn: Optional[str] = None,
                 clickhouse_conn: Optional[Connection] = None,
                 migrations_path: Optional[str] = None,
                 migrations_table: Optional[str] = None,
                 environ: Optional[Dict[str, Any]] = None,
                 ):
        if clickhouse_conn:
            self._conn = clickhouse_conn
        elif clickhouse_dsn:
            self.clickhouse_dsn = clickhouse_dsn
        else:
            raise RuntimeError('clickhouse connection not configure')
        self.migration_path = migrations_path or self.migration_path
        self.migrations_table = migrations_table or self.migrations_table
        self.environ = environ or {}

    @show_migration_error
    async def make(self, name: str, force: bool = False):
        file_migrations = self.file_migrations
        num = 1
        if file_migrations:
            num = file_migrations[len(file_migrations)].version + 1
        pos = self.position(await self.db_meta_migrations())
        if not force and pos + 1 != num:
            raise MigrationError(f'Version: {pos + 1} not applied')
        for action in ('up', 'down'):
            filename = f'{num:0>5d}_{name}.{action}.sql'
            pathlib.Path(os.path.join(self.migration_path, filename)).write_text("")
            print(f'create migration {filename}')

    @show_migration_error
    async def show(self):
        metadata: Dict[int, MigrationRecord] = {}
        db_meta_migrations = await self.db_meta_migrations()
        for m in db_meta_migrations:
            if m.version not in metadata:
                metadata[m.version] = m

        def _cmp_md5(left, right):
            return 'valid' if left == right else 'invalid'

        print('version | name                           | status  | up_md5   | down_md5 | created_at')
        print('-'*102)
        file_migrations = self.file_migrations
        for version in sorted(file_migrations.keys()):
            f = file_migrations[version]
            d = metadata.get(version) or MigrationRecord(version=version)
            created_at = d.created_at or '-'
            if created_at != '-':
                created_at = created_at.isoformat()
            if d.status != Status.PENDING:
                valid_up = _cmp_md5(f.up_md5, d.up_md5)
                valid_down = _cmp_md5(f.down_md5, d.down_md5)
            else:
                valid_up = valid_down = '-'
            print(f'{f.version:<7d} | {f.name:<30s} | {d.status:<7s} | {valid_up:<8s} | {valid_down:<8s} | {created_at:<30s}')
        print('-'*102)
        version = self.position(db_meta_migrations)
        if version > 0:
            print(f'Current apply position version: {file_migrations[version].version} - {file_migrations[version].name}')

    @show_migration_error
    async def force(self, reset=False):
        meta = await self.db_meta_migrations()
        if not meta:
            raise MigrationError('migration not found')
        if meta[0].status not in [Status.DIRTY_UP, Status.DIRTY_DOWN]:
            raise MigrationError('current migration not dirty')
        if not reset:
            await self._log_action(
                version=meta[0].version,
                name=meta[0].name,
                status=Status.UP if meta[0].status == Status.DIRTY_UP else Status.DOWN,
                up_md5=meta[0].up_md5,
                down_md5=meta[0].down_md5,
            )
        else:
            if len(meta) < 2:
                raise MigrationError('prev migration not found')
            await self._log_action(
                version=meta[1].version,
                name=meta[1].name,
                status=meta[1].status,
                up_md5=meta[1].up_md5,
                down_md5=meta[1].down_md5,
            )

    @show_migration_error
    async def up(self, *, step: Optional[int] = None):
        file_migrations = self.file_migrations
        step = step or len(file_migrations)
        pos = self.position(await self.db_meta_migrations())
        play_migration = sorted([k for k in file_migrations.keys() if pos < k <= pos + step])
        for version in play_migration:
            f = file_migrations[version]
            await self._apply_migrate(f, Action.UP)

    @show_migration_error
    async def down(self, *, step: Optional[int] = None):
        file_migrations = self.file_migrations
        step = step or len(file_migrations)
        pos = self.position(await self.db_meta_migrations())
        play_migration = reversed(sorted([k for k in file_migrations.keys() if pos-step < k <= pos]))
        for version in play_migration:
            f = file_migrations[version]
            await self._apply_migrate(f, Action.DOWN)

    async def _apply_migrate(self, meta: MigrationFiles, action: Action):
        print(f'Migrate {action.name} {meta.name}...', end='')
        await self._log_action(
            version=meta.version,
            name=meta.name,
            status=Status.DIRTY_UP if action == Action.UP else Status.DIRTY_DOWN,
            up_md5=meta.up_md5,
            down_md5=meta.down_md5,
        )
        scripts = meta.up if action == Action.UP else meta.down
        scripts = [c for c in [s.strip() for s in scripts.split(self.MULTISTATE_SEPARATOR)] if c]
        for query in scripts:
            query = self._render(query)
            await self._execute(query)
        await self._log_action(
            version=meta.version,
            name=meta.name,
            status=Status.UP if action == Action.UP else Status.DOWN,
            up_md5=meta.up_md5,
            down_md5=meta.down_md5,
        )
        print('OK')

    def _render(self, query: str) -> str:
        return query.format(**self.environ)

    async def _log_action(self, *, version, name, status, up_md5, down_md5):
        await self._execute(f"""INSERT INTO {self.migrations_table}
            (version, name, status, up_md5, down_md5, created_at) 
            VALUES""", [{
            'version': version,
            'name': name,
            'status': status,
            'up_md5': up_md5,
            'down_md5': down_md5,
            'created_at': datetime.datetime.utcnow(),
        }])

    async def _check_database_exist(self):
        print('Check database exists')
        r = urlparse(self.clickhouse_dsn)
        conn = await asynch.connect(
            host=r.hostname,
            port=r.port,
            user=r.username or 'default',
            password=r.password or '')
        db_name = r.path.lstrip("/")
        async with conn.cursor() as cursor:
            await cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
        await conn.close()

    async def _check_connect(self):
        if not self._conn:
            await self._check_database_exist()
            print(f'Connect to {self.clickhouse_dsn}...', end='')
            self._conn = await asynch.connect(dsn=self.clickhouse_dsn)
            print('OK')

    @property
    def _database_name(self):
        return self._conn.database

    async def _execute(self, query, args=None):
        await self._check_connect()
        async with self._conn.cursor(cursor=DictCursor) as cursor:
            res = await cursor.execute(query, args)
            return res, cursor.fetchall()

    async def _check_migration_table(self):
        await self._check_connect()  # init database_name
        print('Migration table exist...', end='')
        _, res = await self._execute(
            f"SHOW TABLES FROM {self._database_name} LIKE '{self.migrations_table}'")
        table_exist = len(res) > 0
        print('OK' if table_exist else 'NOT FOUND')
        return table_exist

    async def _create_migration_table(self):
        print('Create migration table...', end='')
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.migrations_table} (
                version UInt32,
                name String,
                status String, 
                up_md5 String, 
                down_md5 String, 
                created_at DateTime64(9) DEFAULT now64()
            ) Engine=MergeTree ORDER BY (created_at)"""
        await self._execute(query)
        print('CREATED')

    async def db_meta_migrations(self) -> List[MigrationRecord]:
        if not await self._check_migration_table():
            await self._create_migration_table()
        _, res = await self._execute(
            f"""SELECT version, name, status, up_md5, down_md5, created_at 
                FROM `{self.migrations_table}` ORDER BY (created_at) DESC"""
        )
        return [MigrationRecord(**d) for d in res]

    @classmethod
    def position(cls, db_meta_migrations: List[MigrationRecord]) -> int:
        for m in db_meta_migrations:
            if m.status == Status.UP:
                return m.version
            elif m.status == Status.DIRTY_DOWN:
                return m.version - 1
        return 0

    @property
    def file_migrations(self) -> Dict[int, MigrationFiles]:
        if not os.path.exists(self.migration_path):
            os.makedirs(self.migration_path)

        data: Dict[int, MigrationFiles] = {}
        for f in os.scandir(f"{self.migration_path}"):
            if not f.name.endswith('.up.sql'):
                continue
            version = int(f.name.split('_', 1)[0])

            data[version] = MigrationFiles(
                version=version,
                name=remove_suffix(f.name, '.up.sql'),
                up=pathlib.Path(f"{self.migration_path}/{f.name}").read_text(),
            )
            data[version].up_md5 = hashlib.md5(data[version].up.encode()).hexdigest()

        for f in os.scandir(f"{self.migration_path}"):
            if not f.name.endswith('.down.sql'):
                continue
            version = int(f.name.split('_', 1)[0])
            if version not in data:
                raise MigrationError(f'Version: {version} - not found up migration file')
            data[version].down = pathlib.Path(f"{self.migration_path}/{f.name}").read_text()
            data[version].down_md5 = hashlib.md5(data[version].down.encode()).hexdigest()

        keys = list(data.keys())
        keys = sorted(keys)
        for i, version in enumerate(keys):
            if i != version - 1:
                raise MigrationError(f'Version: {version} in position {i} - broken file sequence')
        return data
