import hashlib
import secrets
from uuid import uuid4


async def ensure_project(conn, name):
    result = await conn.execute(
        """
        INSERT INTO projects (id, name) VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id
        """,
        (uuid4(), name),
    )
    return (await result.fetchone())["id"]


async def ensure_run(conn, project_id, name):
    result = await conn.execute(
        """
        INSERT INTO runs (id, project_id, name) VALUES (%s, %s, %s)
        ON CONFLICT (project_id, name) DO UPDATE SET name = EXCLUDED.name RETURNING id
        """,
        (uuid4(), project_id, name),
    )
    return (await result.fetchone())["id"]


async def rotate_key(conn, project_id, scope):
    # Lock the project so two administrators cannot issue competing active keys.
    result = await conn.execute("SELECT id FROM projects WHERE id = %s FOR UPDATE", (project_id,))
    if await result.fetchone() is None:
        raise ValueError("Unknown project")
    await conn.execute(
        "UPDATE api_keys SET revoked_at = now() WHERE project_id = %s AND scope = %s AND revoked_at IS NULL",
        (project_id, scope),
    )
    token = secrets.token_urlsafe(32)
    await conn.execute(
        "INSERT INTO api_keys (id, project_id, scope, digest) VALUES (%s, %s, %s, %s)",
        (uuid4(), project_id, scope, hashlib.sha256(token.encode()).hexdigest()),
    )
    return token
