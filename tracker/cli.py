import argparse
import asyncio
import json
import os

import psycopg
from psycopg.rows import dict_row

from .admin import ensure_project, ensure_run, rotate_key


async def run(args):
    async with await psycopg.AsyncConnection.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as conn:
        if args.command == "rotate":
            row = await (await conn.execute("SELECT id FROM projects WHERE name = %s", (args.project,))).fetchone()
            if row is None:
                raise SystemExit("Unknown project")
            result = {"project": args.project, "scope": args.scope,
                      "key": await rotate_key(conn, row["id"], args.scope)}
        else:
            labels = ["Project A", "Project B"] if args.command == "demo" else [args.project]
            result = []
            for label in labels:
                project_id = await ensure_project(conn, label)
                run_id = await ensure_run(conn, project_id, "Training run" if args.command == "demo" else args.run)
                result.append({"project": label, "run_id": str(run_id),
                               "read_key": await rotate_key(conn, project_id, "read"),
                               "write_key": await rotate_key(conn, project_id, "write")})
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Manage local tracker projects and keys.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="Create two demo projects and issue fresh keys.")
    project = commands.add_parser("project", help="Create a named project/run and issue fresh keys.")
    project.add_argument("project")
    project.add_argument("run")
    rotate = commands.add_parser("rotate", help="Revoke the current key and issue a fresh one.")
    rotate.add_argument("project")
    rotate.add_argument("scope", choices=["read", "write"])
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
