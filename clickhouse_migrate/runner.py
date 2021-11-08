import argparse
import asyncio
import os

from dotenv import dotenv_values

from clickhouse_migrate.migrate import ClickHouseMigrate, Action


def check_positive_int(value):
    val = int(value)
    if val <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return val


async def _run():
    parser = argparse.ArgumentParser("ClickHouse migrate")
    parser.add_argument(
        "--env", type=str, default=".env", help="environment variables (default: .env)"
    )
    parser.add_argument(
        "--dsn",
        type=str,
        default=os.environ.get(
            "CLICKHOUSE_DSN", "clickhouse://localhost:9000/database"
        ),
        help="clickhouse dsn. Env: CLICKHOUSE_DSN (default: clickhouse://localhost:9000/database)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        help="clickhouse host. Env: CLICKHOUSE_HOST (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=os.environ.get("CLICKHOUSE_PORT", 9000),
        help="clickhouse port. Env: CLICKHOUSE_PORT (default: 9000)",
    )
    parser.add_argument(
        "--username",
        type=str,
        default=os.environ.get("CLICKHOUSE_USER", "default"),
        help='clickhouse user. Env: CLICKHOUSE_USER (default: "default")',
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        help='clickhouse password. Env: CLICKHOUSE_USER (default: "")',
    )
    parser.add_argument(
        "--database",
        type=str,
        default=os.environ.get("CLICKHOUSE_DATABASE", ""),
        help='clickhouse database. Env: CLICKHOUSE_DATABASE (default: "")',
    )
    parser.add_argument(
        "--migration-path",
        type=str,
        default=os.environ.get("MIGRATION_PATH", "migrations"),
        help="migration path. Env: MIGRATION_PATH (default: migrations)",
    )
    parser.add_argument(
        "--migration-table",
        type=str,
        default=os.environ.get("MIGRATIONS_TABLE", "schema_migrations"),
        help="migration table. Env: MIGRATIONS_TABLE (default: schema_migrations)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    subparsers = parser.add_subparsers(dest="subparser_name")

    parser_make = subparsers.add_parser("make", help="make new migration")
    parser_make.add_argument(
        "name", type=str, nargs="?", default="new", help="migration name (default: new)"
    )
    parser_make.add_argument(
        "--force", action="store_true", help="force make migration files"
    )

    subparsers.add_parser("show", help="show migrations")

    show_sql = subparsers.add_parser(
        "show_sql", help="show DDL migrations after parse template engine"
    )
    show_sql.add_argument("step", type=check_positive_int, help="migration step")
    show_sql.add_argument("direction", type=str, help="up|down")

    parser_up = subparsers.add_parser("up", help="migrate up")
    parser_up.add_argument(
        "step", type=check_positive_int, nargs="?", help="migration step"
    )

    parser_down = subparsers.add_parser("down", help="migrate down")
    parser_down.add_argument(
        "step", type=check_positive_int, nargs="?", help="migration step"
    )

    subparsers.add_parser("force", help="force last dirty migration mark as success")

    subparsers.add_parser("reset", help="reset last dirty migration")

    args = parser.parse_args()

    environ = {
        **os.environ,
        **dotenv_values(args.env),
    }
    m = ClickHouseMigrate(
        clickhouse_dsn=args.dsn,
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        database=args.database,
        migrations_path=args.migration_path,
        migrations_table=args.migration_table,
        environ=environ,
        verbose=args.verbose,
    )

    if args.subparser_name == "show":
        await m.show()
    elif args.subparser_name == "show_sql":
        await m.show_sql(
            args.step, Action.DOWN if args.direction.lower() == "down" else Action.UP
        )
    elif args.subparser_name == "make":
        await m.make(args.name, args.force)
    elif args.subparser_name == "up":
        await m.up(step=args.step)
    elif args.subparser_name == "down":
        await m.down(step=args.step)
    elif args.subparser_name == "force":
        await m.force()
    elif args.subparser_name == "reset":
        await m.force(reset=True)
    else:
        parser.print_help()


def run():
    asyncio.run(_run())


if __name__ == "__main__":
    run()
