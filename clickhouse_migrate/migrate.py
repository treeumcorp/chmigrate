import datetime
import hashlib
import os
import logging
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


logger = logging.getLogger(__name__)


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
            logger.error(e)

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
    connection_params = {
        "host": "localhost",
        "port": 9000,
        "username": "default",
        "password": "",
        "secure": False,
        "ca_certs": "",
    }
    database: str = ""
    _is_database_exists = False
    migrations_table: str = "schema_migrations"
    migration_path: str = "migrations"

    environ: Dict[str, Any] = {}

    MULTISTATE_SEPARATOR = ";"

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
        secure: bool = False,
        ca_certs: str = "",
    ):

        if clickhouse_conn:
            self.database = clickhouse_conn.database
            self.connection_params.update(
                {
                    "host": clickhouse_conn.host,
                    "port": clickhouse_conn.port,
                    "user": clickhouse_conn.user,
                    "password": clickhouse_conn.password,
                }
            )
        elif clickhouse_dsn:
            r = urlparse(clickhouse_dsn)
            self.database = r.path.strip("/")
            self.connection_params.update(
                {
                    "host": r.hostname,
                    "port": r.port,
                    "user": r.username,
                    "password": r.password,
                }
            )
        elif all([host, port, database, username]):
            self.database = database
            self.connection_params.update(
                {
                    "host": host,
                    "port": port,
                    "user": username,
                    "password": password,
                }
            )
        else:
            raise RuntimeError("clickhouse connection not configure")

        self.connection_params.update(
            {
                "secure": secure,
                "ca_certs": ca_certs,
            }
        )

        self.migration_path = migrations_path or self.migration_path
        self.migrations_table = migrations_table or self.migrations_table
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.migration_path),
        )
        self.environ = environ or {}

        logger.debug(f"init params: {self.connection_params}")

    async def _check_database_exist(self):
        if not self._is_database_exists:
            conn = await asynch.connect(**self.connection_params)
            async with conn.cursor() as cursor:
                await cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
                self.is_database_exists = True

    async def _execute(self, query, args=None):
        conn = await asynch.connect(database=self.database, **self.connection_params)
        async with conn.cursor(cursor=DictCursor) as cursor:
            res = await cursor.execute(query, args)
            return res, cursor.fetchall()

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
            logger.info(f"create migration {filename}")

    @show_migration_error
    async def show(self):
        metadata: Dict[int, MigrationRecord] = {}
        db_meta_migrations = await self.db_meta_migrations()
        for m in db_meta_migrations:
            if m.version not in metadata:
                metadata[m.version] = m

        def _cmp_md5(left, right):
            return "valid" if left == right else "invalid"

        logger.info(
            "version | name                           | status  | up_md5   | down_md5 | created_at"
        )
        logger.info("-" * 102)
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
            logger.info(
                f"{f.version:<7d} | {f.name:<30s} | {d.status:<7s} | {valid_up:<8s} | {valid_down:<8s} | {created_at:<30s}"
            )
        logger.info("-" * 102)
        version = self.position(db_meta_migrations)
        if version > 0:
            logger.info(
                f"Current apply position version: "
                f"{file_migrations[version].version} - "
                f"{file_migrations[version].name}: "
                f"{db_meta_migrations[0].status}"
            )

    async def show_sql(self, step: int, action: Action = Action.UP):
        file_migrations = self.file_migrations
        if len(file_migrations) < step:
            logger.info("migration not found")
        scripts = (
            self._render(file_migrations[step - 1].up_filename)
            if action == Action.UP
            else self._render(file_migrations[step - 1].down_filename)
        )
        logger.info(scripts)

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
        logger.info(f"Start migrate {action.name} {meta.name}")
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
        logger.info(f"Start migrate {action.name} {meta.name} - Complete")

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

    async def _check_migration_table(self):
        logger.debug("Check migration table")
        _, res = await self._execute(
            f"SHOW TABLES FROM `{self.database}` LIKE '{self.migrations_table}'"
        )
        table_exist = len(res) > 0
        logger.debug("Migration table %s", "OK" if table_exist else "NOT FOUND")
        return table_exist

    async def _create_migration_table(self):
        logger.debug("Create migration table...")
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
        logger.debug("Create migration table...CREATED")

    async def db_meta_migrations(self) -> List[MigrationRecord]:
        await self._check_database_exist()
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
