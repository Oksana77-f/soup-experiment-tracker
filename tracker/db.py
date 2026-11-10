import hashlib
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .models import Batch


class Conflict(Exception):
    pass


class MissingRun(Exception):
    pass


@dataclass
class BatchWriteTimings:
    pool_wait_ms: float | None = None
    run_lock_ms: float | None = None
    commit_ms: float | None = None


def elapsed_ms(start_ns: int) -> float:
    return round((perf_counter_ns() - start_ns) / 1_000_000, 3)


SCHEMA = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")


async def initialize(pool):
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(388412615)")
            await conn.execute(SCHEMA)


async def find_key(pool, token, scope):
    digest = hashlib.sha256(token.encode()).hexdigest()
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT id, project_id FROM api_keys WHERE digest = %s AND scope = %s AND revoked_at IS NULL",
            (digest, scope),
        )
        return await result.fetchone()


async def list_runs(pool, key_id):
    async with pool.connection() as conn:
        result = await conn.execute(
            """
            SELECT p.name AS project,
                   COALESCE(jsonb_agg(jsonb_build_object('id', r.id, 'name', r.name)
                       ORDER BY r.name) FILTER (WHERE r.id IS NOT NULL), '[]'::jsonb) AS runs
            FROM api_keys k JOIN projects p ON p.id = k.project_id
            LEFT JOIN runs r ON r.project_id = p.id
            WHERE k.id = %s AND k.scope = 'read' AND k.revoked_at IS NULL
            GROUP BY p.id
            """,
            (key_id,),
        )
        return await result.fetchone()


async def run_page(pool, key_id, run_id, after=0, limit=None):
    async with pool.connection() as conn:
        result = await conn.execute(
            """
            SELECT r.id, r.name, r.last_seq, page.points
            FROM api_keys k
            LEFT JOIN runs r ON r.project_id = k.project_id AND r.id = %s
            CROSS JOIN LATERAL (
                SELECT COALESCE(jsonb_agg(to_jsonb(m) ORDER BY m.seq), '[]'::jsonb) AS points
                FROM (
                    SELECT seq, name, step, value FROM metrics
                    WHERE run_id = r.id AND seq > %s ORDER BY seq LIMIT %s
                ) m
            ) page
            WHERE k.id = %s AND k.scope = 'read' AND k.revoked_at IS NULL
            """,
            (run_id, after, limit, key_id),
        )
        return await result.fetchone()


async def lock_run(conn, run_id, project_id):
    result = await conn.execute(
        "SELECT last_seq FROM runs WHERE id = %s AND project_id = %s FOR UPDATE",
        (run_id, project_id),
    )
    return await result.fetchone()


async def insert_points(conn, run_id, last_seq, new_points):
    if not new_points:
        return last_seq
    result = await conn.execute(
        "UPDATE runs SET last_seq = last_seq + %s WHERE id = %s RETURNING last_seq",
        (len(new_points), run_id),
    )
    last_seq = (await result.fetchone())["last_seq"]
    first_seq = last_seq - len(new_points) + 1
    async with conn.cursor() as cursor:
        await cursor.executemany(
            "INSERT INTO metrics (run_id, seq, name, step, value) VALUES (%s, %s, %s, %s, %s)",
            [(run_id, seq, point.name, point.step, point.value)
             for seq, point in enumerate(new_points, first_seq)],
        )
    return last_seq


async def save_batch(
    pool: AsyncConnectionPool,
    project_id: UUID,
    run_id: UUID,
    batch: Batch,
    digest: str,
    *,
    timings: BatchWriteTimings | None = None,
) -> dict[str, str | int]:
    pool_started = perf_counter_ns() if timings is not None else 0
    async with pool.connection() as conn:
        if timings is not None:
            timings.pool_wait_ms = elapsed_ms(pool_started)
        async with conn.transaction():
            lock_started = perf_counter_ns() if timings is not None else 0
            run = await lock_run(conn, run_id, project_id)
            if timings is not None:
                timings.run_lock_ms = elapsed_ms(lock_started)
            if run is None:
                raise MissingRun

            acknowledgment = await _store_batch(conn, run_id, batch, digest, run["last_seq"])
            commit_started = perf_counter_ns() if timings is not None else 0
        if timings is not None:
            timings.commit_ms = elapsed_ms(commit_started)
    return acknowledgment


async def _store_batch(
    conn: AsyncConnection,
    run_id: UUID,
    batch: Batch,
    digest: str,
    last_seq: int,
) -> dict[str, str | int]:
    result = await conn.execute(
        "SELECT digest, acknowledgment FROM receipts WHERE run_id = %s AND batch_id = %s",
        (run_id, batch.batch_id),
    )
    receipt = await result.fetchone()
    if receipt is not None:
        if receipt["digest"] != digest:
            raise Conflict
        return receipt["acknowledgment"]

    incoming = {}
    for point in batch.points:
        key = (point.name, point.step)
        if key in incoming and incoming[key].value != point.value:
            raise Conflict
        incoming[key] = point

    result = await conn.execute(
        """
        SELECT m.name, m.step, m.value FROM metrics m
        JOIN unnest(%s::text[], %s::bigint[]) AS wanted(name, step)
            USING (name, step)
        WHERE m.run_id = %s
        """,
        ([name for name, _ in incoming], [step for _, step in incoming], run_id),
    )
    existing = {(row["name"], row["step"]): row["value"] for row in await result.fetchall()}
    new_points = []
    for key, point in incoming.items():
        if key not in existing:
            new_points.append(point)
        elif existing[key] != point.value:
            raise Conflict

    last_seq = await insert_points(conn, run_id, last_seq, new_points)
    ack = {"batch_id": str(batch.batch_id), "inserted": len(new_points), "last_seq": last_seq}
    await conn.execute(
        "INSERT INTO receipts (run_id, batch_id, digest, acknowledgment) VALUES (%s, %s, %s, %s)",
        (run_id, batch.batch_id, digest, Jsonb(ack)),
    )
    return ack
