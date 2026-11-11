import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
import pytest

from conftest import auth, batch, live_server, login, point, query, seed
from tracker import db
from tracker.app import create_app


async def hold_writer(phase: str) -> None:
    operation = "_store_batch" if phase == "before" else "save_batch"
    write = getattr(db, operation)

    async def hold(*args, **kwargs):
        receipt = await write(*args, **kwargs)
        print(json.dumps(receipt), flush=True)
        await asyncio.Event().wait()

    # Only the child HTTP worker has the barrier. save_batch returns after COMMIT.
    setattr(db, operation, hold)
    async with live_server(create_app()) as url:
        print(url, flush=True)
        await asyncio.Event().wait()


@pytest.mark.parametrize("phase", ["before", "after"], ids=["before-commit", "after-commit-before-response"])
async def test_writer_killed_at_commit_boundary(api, settings, phase):
    _, client = api
    target = await seed(settings.database_url)
    payload = json.dumps(batch(point(1), point(2))).encode()
    run_path = f"/runs/{target.run}"
    post_options = {"content": payload, "headers": {**auth(target.write), "Content-Type": "application/json"}}
    script = Path(__file__).resolve()
    writer = await asyncio.create_subprocess_exec(
        sys.executable, str(script), phase, stdout=asyncio.subprocess.PIPE,
        env={**os.environ, "DATABASE_URL": settings.database_url, "SESSION_SECRET": settings.session_secret,
             "PYTHONPATH": str(script.parents[1])},
    )
    pending = None
    async with httpx.AsyncClient(timeout=15) as sender:
        try:
            url = (await asyncio.wait_for(writer.stdout.readline(), 15)).decode().strip()
            assert url.startswith("http://127.0.0.1:")
            pending = asyncio.create_task(sender.post(url + run_path + "/metrics", **post_options))
            staged_receipt = json.loads(await asyncio.wait_for(writer.stdout.readline(), 10))
            assert staged_receipt["inserted"] == staged_receipt["last_seq"] == 2
        finally:
            if writer.returncode is None:
                writer.kill()
            await asyncio.wait_for(writer.wait(), 5)
            if pending is not None:
                interrupted, = await asyncio.gather(pending, return_exceptions=True)
    assert writer.returncode != 0
    assert isinstance(interrupted, httpx.RemoteProtocolError)

    await login(client, target)
    before_retry = (await client.get(run_path)).json()
    receipts = await query(settings.database_url, "SELECT acknowledgment FROM receipts WHERE run_id = %s",
                           (target.run,))
    if phase == "before":
        assert before_retry["last_seq"] == 0 and before_retry["points"] == [] and receipts == []
    else:
        assert before_retry["last_seq"] == 2 and len(before_retry["points"]) == 2
        assert receipts == [{"acknowledgment": staged_receipt}]

    response = await client.post(run_path + "/metrics", **post_options)
    assert response.status_code == 200 and response.json() == staged_receipt
    after_retry = (await client.get(run_path)).json()
    assert after_retry["last_seq"] == 2
    assert [point["step"] for point in after_retry["points"]] == [1, 2]
    if phase == "after":
        assert after_retry == before_retry
    print(f"Writer exited with {writer.returncode} {phase} COMMIT; HTTP response lost. "
          f"Points before/after retry: {len(before_retry['points'])}/{len(after_retry['points'])}.")


if __name__ == "__main__":
    asyncio.run(hold_writer(sys.argv[1]))
