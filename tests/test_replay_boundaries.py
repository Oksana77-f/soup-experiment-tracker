import asyncio
import json

import httpx

from conftest import batch, frames, live_server, login, point, post, query, seed
from tracker.app import create_app


async def test_reconnect_drains_multiple_pages_then_receives_live_point(settings):
    async with live_server(create_app(settings)) as url:
        target = await seed(settings.database_url)
        stream_path = f"/runs/{target.run}/events?after=0"
        async with httpx.AsyncClient(base_url=url) as writer, httpx.AsyncClient(base_url=url) as viewer:
            await login(viewer, target)
            assert (await post(writer, target, batch(point(0)))).status_code == 200
            async with viewer.stream("GET", stream_path) as stream:
                assert stream.status_code == 200
                first = await anext(frames(stream))
                assert first["id"] == "1"

            # More than two production pages accumulate while the viewer is disconnected.
            last_offline_step = 2 * settings.page_size + 7
            offline = batch(*(point(step) for step in range(1, last_offline_step + 1)))
            acknowledgment = await post(writer, target, offline)
            assert acknowledgment.status_code == 200
            assert acknowledgment.json()["inserted"] == last_offline_step

            async with viewer.stream("GET", stream_path, headers={"Last-Event-ID": first["id"]}) as stream:
                assert stream.status_code == 200
                replay = frames(stream)
                async with asyncio.timeout(10):
                    recovered = [await anext(replay) for _ in range(last_offline_step)]
                    live = batch(point(last_offline_step + 1))
                    assert (await post(writer, target, live)).status_code == 200
                    recovered.append(await anext(replay))

        assert [int(frame["id"]) for frame in recovered] == list(range(2, last_offline_step + 3))
        stored = await query(settings.database_url, """
            SELECT seq, step, name, value FROM metrics
            WHERE run_id = %s AND seq > 1 ORDER BY seq
        """, (target.run,))
        assert [json.loads(frame["data"]) for frame in recovered] == stored
