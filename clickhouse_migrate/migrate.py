import datetime
import hashlib
import os
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

import asynch
from asynch.connection import Connection
from asynch.cursors import DictCursor
from jinja2 import Environment, FileSystemLoader

from clickhouse_migrate.utils import remove_suffix


class VerboseLevel(Enum):
    DEBUG = 0
    INFO = 10


class MigrationError(Exception):
    message = "migration error"

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
    PENDING = "pending"
    DIRTY_UP = "dirty_up"
    UP = "up"
    DIRTY_DOWN = "dirty_down"
    DOWN = "down"


class Action(StrEnum):
    UP = "up"
    DOWN = "down"


@dataclass
class MigrationFiles:
    version: int
    name: str
    up: Optional[str] = None
    up_md5: Optional[str] = None
    up_filename: Optional[str] = None
    down: Optional[str] = None
    down_md5: Optional[str] = None
    down_filename: Optional[str] = None


@dataclass
class MigrationRecord:
    version: int
    name: str = ""
    status: Status = Status.PENDING
    created_at: Optional[datetime.datetime] = None
    up_md5: Optional[str] = None
    down_md5: Optional[str] = None


class ClickHouseMigrate:
    clickhouse_dsn: str = ""
    host: str = "localhost"
    port: int = 9000
    database: str = ""
    username: str = "default"
    password: str = ""
    migrations_table: str = "schema_migrations"
    migration_path: str = "migrations"

    environ: Dict[str, Any] = {}

    _conn = None

    MULTISTATE_SEPARATOR = ";"

    verbose_level: VerboseLevel = VerboseLevel.INFO

    def __init__(
        self,
        *,
        clickhouse_dsn: Optional[str] = None,
        clickhouse_conn: Optional[Connection] = None,
        host: str = "localhost",
        port: int = 9000,
        database: str = "",
        username: str = "default",
        password: str = "",
        migrations_path: Optional[str] = None,
        migrations_table: Optional[str] = None,
        environ: Optional[Dict[str, Any]] = None,
        verbose: bool = False,
    ):
        if verbose:
            self.verbose_level = VerboseLevel.DEBUG
        if clickhouse_conn:
            self._conn = clickhouse_conn
            self._parse_conn()
        elif clickhouse_dsn:
            self.clickhouse_dsn = clickhouse_dsn
            self._parse_dsn()
        elif all([host, port, database, username]):
            self.host = host
            self.port = port
            self.database = database
            self.username = username
            self.password = password
        else:
            raise RuntimeError("clickhouse connection not configure")
        self.migration_path = migrations_path or self.migration_path
        self.migrations_table = migrations_table or self.migrations_table
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.migration_path),
        )
        self.environ = environ or {}

    def _parse_conn(self):
        if self._conn:
            self.host = self._conn.host
            self.port = self._conn.port
            self.username = self._conn.user
            self.password = self._conn.password
            self.database = self._conn.database

    def _parse_dsn(self):
        if self.clickhouse_dsn:
            r = urlparse(self.clickhouse_dsn)
            self.host = r.hostname
            self.port = r.port
            self.username = r.username or "default"
            self.password = r.password or ""
            self.database = r.path.strip("/")

    def _print(self, msg, **kwargs):
        if self.verbose_level.value >= VerboseLevel.INFO.value:
            print(msg, **kwargs)

    def _debug(self, msg, **kwargs):
        if self.verbose_level.value >= VerboseLevel.DEBUG.value:
            print(msg, **kwargs)

    @show_migration_error
    async def make(self, name: str, force: bool = False):
        file_migrations = self.file_migrations
        num = 1
        if file_migrations:
            num = file_migrations[len(file_migrations)].version + 1
        pos = self.position(await self.db_meta_migrations())
        if not force and pos + 1 != num:
            raise MigrationError(f"Version: {pos + 1} not applied")
        for action in ("up", "down"):
            filename = f"{num:0>5d}_{name}.{action}.sql"
            Path(os.path.join(self.migration_path, filename)).write_text("")
            self._print(f"create migration {filename}")

    @show_migration_error
    async def show(self):
        metadata: Dict[int, MigrationRecord] = {}
        db_meta_migrations = await self.db_meta_migrations()
        for m in db_meta_migrations:
            if m.version not in metadata:
                metadata[m.version] = m

        def _cmp_md5(left, right):
            return "valid" if left == right else "invalid"

        self._print(
            "version | name                           | status  | up_md5   | down_md5 | created_at"
        )
        self._print("-" * 102)
        file_migrations = self.file_migrations
        for version in sorted(file_migrations.keys()):
            f = file_migrations[version]
            d = metadata.get(version) or MigrationRecord(version=version)
            created_at = d.created_at or "-"
            if created_at != "-":
                created_at = created_at.isoformat()
            if d.status != Status.PENDING:
                valid_up = _cmp_md5(f.up_md5, d.up_md5)
                valid_down = _cmp_md5(f.down_md5, d.down_md5)
            else:
                valid_up = valid_down = "-"
            self._print(
                f"{f.version:<7d} | {f.name:<30s} | {d.status:<7s} | {valid_up:<8s} | {valid_down:<8s} | {created_at:<30s}"
            )
        self._print("-" * 102)
        version = self.position(db_meta_migrations)
        if version > 0:
            self._print(
                f"Current apply position version: "
                f"{file_migrations[version].version} - "
                f"{file_migrations[version].name}: "
                f"{db_meta_migrations[0].status}"
            )

    async def show_sql(self, step: int, action: Action = Action.UP):
        file_migrations = self.file_migrations
        if len(file_migrations) < step:
            print("migration not found")
        scripts = (
            self._render(file_migrations[step - 1].up_filename)
            if action == Action.UP
            else self._render(file_migrations[step - 1].down_filename)
        )
        print(scripts)

    @show_migration_error
    async def force(self, reset=False):
        meta = await self.db_meta_migrations()
        if not meta:
            raise MigrationError("migration not found")
        if meta[0].status not in [Status.DIRTY_UP, Status.DIRTY_DOWN]:
            raise MigrationError("current migration not dirty")
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
                raise MigrationError("prev migration not found")
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
        migrations = await self.db_meta_migrations()
        self._check_current_migration(migrations)
        pos = self.position(migrations)
        play_migration = sorted(
            [k for k in file_migrations.keys() if pos < k <= pos + step]
        )
        for version in play_migration:
            f = file_migrations[version]
            await self._apply_migrate(f, Action.UP)

    @show_migration_error
    async def down(self, *, step: Optional[int] = None):
        file_migrations = self.file_migrations
        step = step or len(file_migrations)
        migrations = await self.db_meta_migrations()
        self._check_current_migration(migrations)
        pos = self.position(migrations)
        play_migration = reversed(
            sorted([k for k in file_migrations.keys() if pos - step < k <= pos])
        )
        for version in play_migration:
            f = file_migrations[version]
            await self._apply_migrate(f, Action.DOWN)

    @classmethod
    def _check_current_migration(cls, migrations: List[MigrationRecord]):
        if len(migrations) > 0 and migrations[0].status in [
            Status.DIRTY_UP,
            Status.DIRTY_DOWN,
        ]:
            raise MigrationError(f"Current migration {migrations[-1].status}")

    async def _apply_migrate(self, meta: MigrationFiles, action: Action):
        self._print(f"Migrate {action.name} {meta.name}...", end="")
        await self._log_action(
            version=meta.version,
            name=meta.name,
            status=Status.DIRTY_UP if action == Action.UP else Status.DIRTY_DOWN,
            up_md5=meta.up_md5,
            down_md5=meta.down_md5,
        )
        scripts = (
            self._render(meta.up_filename)
            if action == Action.UP
            else self._render(meta.down_filename)
        )
        scripts = [
            c
            for c in [s.strip() for s in scripts.split(self.MULTISTATE_SEPARATOR)]
            if c
        ]
        for query in scripts:
            await self._execute(query)
        await self._log_action(
            version=meta.version,
            name=meta.name,
            status=Status.UP if action == Action.UP else Status.DOWN,
            up_md5=meta.up_md5,
            down_md5=meta.down_md5,
        )
        self._print("OK")

    def _render(self, filename: str) -> str:
        template = self.jinja_env.get_template(filename)
        return template.render(**self.environ)

    async def _log_action(self, *, version, name, status, up_md5, down_md5):
        await self._execute(
            f"""INSERT INTO {self.migrations_table}
            (version, name, status, up_md5, down_md5, created_at) 
            VALUES""",
            [
                {
                    "version": version,
                    "name": name,
                    "status": status,
                    "up_md5": up_md5,
                    "down_md5": down_md5,
                    "created_at": datetime.datetime.utcnow(),
                }
            ],
        )

    async def _check_database_exist(self):
        self._print("Check database exists")
        conn = await asynch.connect(
            host=self.host, port=self.port, user=self.username, password=self.password
        )
        async with conn.cursor() as cursor:
            await cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        await conn.close()

    async def conn(self):
        if not self._conn:
            await self._check_database_exist()
            if self.clickhouse_dsn:
                self._debug(f"Connect to {self.clickhouse_dsn}...", end="")
                self._conn = await asynch.connect(dsn=self.clickhouse_dsn)
                self._debug("OK")
            else:
                self._debug(f"Connect to {self.host}:{self.port}...", end="")
                self._conn = await asynch.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    database=self.database,
                )
                self._debug("OK")
        return self._conn

    async def _execute(self, query, args=None):
        conn = await self.conn()
        async with conn.cursor(cursor=DictCursor) as cursor:
            res = await cursor.execute(query, args)
            return res, cursor.fetchall()

    async def _check_migration_table(self):
        self._print("Migration table exist...", end="")
        _, res = await self._execute(
            f"SHOW TABLES FROM {self.database} LIKE '{self.migrations_table}'"
        )
        table_exist = len(res) > 0
        self._print("OK" if table_exist else "NOT FOUND")
        return table_exist

    async def _create_migration_table(self):
        self._print("Create migration table...", end="")
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
        self._print("CREATED")

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
            if m.status == Status.DOWN:
                return m.version - 1
            else:
                return m.version
        return 0

    @property
    def file_migrations(self) -> Dict[int, MigrationFiles]:
        if not os.path.exists(self.migration_path):
            os.makedirs(self.migration_path)

        data: Dict[int, MigrationFiles] = {}
        for f in os.scandir(f"{self.migration_path}"):
            filename = f.name
            if not filename.endswith(".up.sql"):
                continue
            version = int(filename.split("_", 1)[0])

            data[version] = MigrationFiles(
                version=version,
                name=remove_suffix(filename, ".up.sql"),
                up=Path(f"{self.migration_path}/{filename}").read_text(),
                up_filename=filename,
            )
            data[version].up_md5 = hashlib.md5(data[version].up.encode()).hexdigest()

        for f in os.scandir(f"{self.migration_path}"):
            filename = f.name
            if not filename.endswith(".down.sql"):
                continue
            version = int(filename.split("_", 1)[0])
            if version not in data:
                raise MigrationError(
                    f"Version: {version} - not found up migration file"
                )
            data[version].down = Path(f"{self.migration_path}/{filename}").read_text()
            data[version].down_md5 = hashlib.md5(
                data[version].down.encode()
            ).hexdigest()
            data[version].down_filename = filename

        keys = list(data.keys())
        keys = sorted(keys)
        for i, version in enumerate(keys):
            if i != version - 1:
                raise MigrationError(
                    f"Version: {version} in position {i} - broken file sequence"
                )
        return data
