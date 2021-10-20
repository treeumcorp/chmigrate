import argparse
import asyncio
import os

from clickhouse_migrate.migrate import ClickHouseMigrate


def check_positive_int(value):
    val = int(value)
    if val <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return val


async def _run():
    m = ClickHouseMigrate(
        dsn=os.environ.get('CLICKHOUSE_DSN', 'clickhouse://localhost:9005/dbmigrate'),
        migrations_path=os.environ.get('MIGRATION_PATH', 'migrations'),
        migrations_table=os.environ.get('MIGRATIONS_TABLE', 'schema_migrations'),
    )

    parser = argparse.ArgumentParser('ClickHouse migrate')
    subparsers = parser.add_subparsers(dest='subparser_name')

    parser_make = subparsers.add_parser('make', help='make new migration')
    parser_make.add_argument('name', type=str, nargs='?', default='new', help='migration name')
    parser_make.add_argument('--force', action='store_true', help='force make migration files')

    subparsers.add_parser('show', help='show migrations')

    parser_up = subparsers.add_parser('up', help='migrate up')
    parser_up.add_argument('step', type=check_positive_int, nargs='?', help='migration step')

    parser_down = subparsers.add_parser('down', help='migrate down')
    parser_down.add_argument('step', type=check_positive_int, nargs='?', help='migration step')

    subparsers.add_parser('force', help='force last dirty migration mark as success')

    subparsers.add_parser('reset', help='reset last dirty migration')

    args = parser.parse_args()

    if args.subparser_name == 'show':
        await m.show()
    elif args.subparser_name == 'make':
        await m.make(args.name, args.force)
    elif args.subparser_name == 'up':
        await m.up(step=args.step)
    elif args.subparser_name == 'down':
        await m.down(step=args.step)
    elif args.subparser_name == 'force':
        await m.force()
    elif args.subparser_name == 'reset':
        await m.force(reset=True)
    else:
        parser.print_help()


def run():
    asyncio.run(_run())


if __name__ == "__main__":
    run()
