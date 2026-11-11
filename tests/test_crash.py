import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path
from uuid import UUID

from tracker import db
from tracker.app import create_app
from tracker.models import Batch


async def hold_before_commit(project_id: UUID, run_id: UUID, payload: bytes) -> None:
    app = create_app()
    store = db._store_batch

    async def hold(*args):
        await store(*args)
        print("Metrics, cursor and receipt written before COMMIT", flush=True)
        await asyncio.Event().wait()

    # This child is the only process with the barrier in its write path.
    db._store_batch = hold
    async with app.router.lifespan_context(app):
        await db.save_batch(app.state.pool, project_id, run_id, Batch.model_validate_json(payload),
                            hashlib.sha256(payload).hexdigest())


async def test_writer_killed_before_commit_leaves_no_partial_batch(api, settings):
    from conftest import auth, batch, login, point, query, seed

    _, client = api
    target = await seed(settings.database_url)
    payload = json.dumps(batch(point(1), point(2))).encode()
    script = Path(__file__).resolve()
    writer = await asyncio.create_subprocess_exec(
        sys.executable, str(script), str(target.project), str(target.run),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        env={**os.environ, "DATABASE_URL": settings.database_url, "SESSION_SECRET": settings.session_secret,
             "PYTHONPATH": str(script.parents[1])},
    )
    try:
        writer.stdin.write(payload)
        await writer.stdin.drain()
        writer.stdin.close()
        marker = await asyncio.wait_for(writer.stdout.readline(), timeout=15)
        assert marker.rstrip() == b"Metrics, cursor and receipt written before COMMIT"
    finally:
        if writer.returncode is None:
            writer.kill()
        await asyncio.wait_for(writer.wait(), timeout=5)
    assert writer.returncode != 0

    await login(client, target)
    run_path = f"/runs/{target.run}"
    state = (await client.get(run_path)).json()
    assert state["points"] == [] and state["last_seq"] == 0
    assert await query(settings.database_url, "SELECT 1 FROM receipts WHERE run_id = %s", (target.run,)) == []
    response = await client.post(run_path + "/metrics", content=payload,
                                 headers={**auth(target.write), "Content-Type": "application/json"})
    assert response.status_code == 200
    assert response.json()["inserted"] == response.json()["last_seq"] == 2
    print(f"Writer exited with {writer.returncode} before COMMIT. Snapshot and receipts empty. Retry inserted 2 points.")


if __name__ == "__main__":
    asyncio.run(hold_before_commit(UUID(sys.argv[1]), UUID(sys.argv[2]), sys.stdin.buffer.read()))
