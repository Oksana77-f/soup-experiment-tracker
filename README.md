# Soup Experiment Tracker

A small, project-scoped tracker for batched training metrics. The browser reads a PostgreSQL snapshot and receives committed metrics over SSE. See [the submission note](docs/submission.md) ([PDF](docs/submission.pdf)), [the design](docs/design.md), [the working plan in Russian](docs/plan-ru.md) and [the AI-use log](docs/ai-usage-ru.md).

## Run the demo

```sh
docker compose up -d --build --wait
docker compose exec app python -m tracker.cli demo
```

Open [localhost:8000](http://127.0.0.1:8000). Sign in with one project's `read_key` and open its training run. Run the sender using that project's `write_key` and `run_id` from the CLI output.

If port 8000 is already in use, run `TRACKER_PORT=18060 docker compose up -d --build --wait` and open [localhost:18060](http://127.0.0.1:18060). This changes the browser address while the sender in the app container still uses port 8000.

```sh
read -rsp "Write key: " SOUP_WRITE_KEY; echo; export SOUP_WRITE_KEY
docker compose exec -e SOUP_WRITE_KEY app python -m tracker.send RUN_ID
unset SOUP_WRITE_KEY
```

To generate more points, pass `--start 30` after the first default run. To rotate a read key, run `docker compose exec app python -m tracker.cli rotate "Project A" read`. The demo command issues fresh keys each time. Keep keys out of source control. On this Windows machine Docker is available in the Ubuntu-24.04 WSL distribution.

## API

The sender uses `Authorization: Bearer <write_key>` and posts JSON to `POST /runs/{id}/metrics`. A successful response includes `inserted` and `last_seq`. Use a UUID `batch_id` and up to 256 finite metric points. Retries use the same serialized body. The demo sender holds one pending batch in memory while it retries.

```json
{
  "batch_id": "68e089c7-3e54-4222-bfa6-0c98cdf39258",
  "points": [
    {"name": "loss", "step": 120, "value": 0.42},
    {"name": "loss", "step": 119, "value": 0.45}
  ]
}
```

Read keys are exchanged for an expiring cookie at `POST /api/session`. The UI uses `GET /runs`, `GET /runs/{id}` and `GET /runs/{id}/events?after=0`. The app uses one active read key and one active write key per project.

Signing out revokes the presented session in PostgreSQL, closes its local SSE streams and clears the cookie. Sessions issued independently under the same read key remain valid. Browser tabs recheck access after a sign-in or sign-out in another tab. Older cookies remain readable until their original expiry or revocation.

## Inspect batch write timings

Set `SOUP_LOG_POST_TIMINGS=1` for a local measurement run. This enables an internal JSON log for authenticated, valid batches that reach the write path.

```sh
SOUP_LOG_POST_TIMINGS=1 docker compose up -d --build --wait app
docker compose logs --no-log-prefix app | grep post_metrics_timing
```

- `total_ms` runs from the start of the handler through the write and includes authentication and validation.
- `pool_wait_ms` measures acquisition of the write connection. The connection used for authorization is separate.
- `run_lock_ms` measures the entire `SELECT … FOR UPDATE` call, including its database round trip.
- `commit_ms` measures successful transaction exit before returning the connection to the pool. A rejected or rolled-back batch leaves this field `null`. Any phase that does not complete is also `null`.

The flag defaults to off. When reporting a load run, record whether it was enabled because writing logs adds work. Timing records contain only the outcome and durations.

## Inspect SSE timings

Set `SOUP_LOG_SSE_DIAGNOSTICS=1` for a local load run. Stop the app after the driver finishes and read its `sse_diagnostics` log. Start a fresh app process for each profile so the counters cover one run.

```sh
SOUP_LOG_SSE_DIAGNOSTICS=1 docker compose up -d --build --wait app
# Run the benchmark below.
docker compose stop app
docker compose logs --no-log-prefix app | grep sse_diagnostics
```

- `loop_delay` samples the event loop's wake-up delay on a 20 ms timer.
- `page_read` covers the SSE page query, including pool acquisition, authorization and result decoding. Counts include initial reads, polling and replay.
- `data_send` covers the ASGI send await for metric frames. It includes server backpressure and ends at the ASGI server boundary.

The log reports sample counts, fixed-bucket percentile upper bounds and maximums in milliseconds. It includes app startup and stream drain. A failed or cancelled read or send contributes its elapsed time. The flag defaults to off and the log contains no credentials or metric payloads.


## Verify

```sh
docker compose --profile test run --rm --build test
docker compose --profile test run --rm -e TRACKER_TEST_MUTATION=skip-replay test \
  python -m pytest -q 'tests/test_stream.py::test_browser_matches_input_and_database_after_disconnect[native-reconnect]'
docker compose --profile test run --rm -e TRACKER_TEST_MUTATION=no-run-lock test \
  python -m pytest -q tests/test_api.py::test_concurrent_batches_share_one_point
```

The last two commands should fail. They change behavior only inside their test fixtures. The test service uses a separate `soup_tests` database. The browser checks compare actual DOM rows, stored data and input. The deterministic network test closes an HTTP SSE response from the server, then holds the next request while it writes new points. A separate variant returns one `502`. The slow-client tests stall ASGI sends of data, heartbeat and final body and verify replay afterward.

The [fresh-clone check](evidence/fresh-clone-compose.txt) records the browser demo. The [earlier `--no-cache` run](evidence/nocache-check.txt) passed 18 tests on freshly built images. The [integrated check](evidence/criteria-final.txt) passed 29 tests with the current app build and the prior test dependency image. It includes a non-reading Linux loopback TCP client, copied-cookie revocation after logout and replay through multiple pages.

Run the benchmark on a local instance with a disposable demo database. It creates a separate benchmark project for every invocation.

```sh
docker compose --profile test run --rm \
  -e DATABASE_URL=postgresql://soup:local-compose-only@db:5432/soup \
  test python -m scripts.load --url http://app:8000 --seconds 60 --viewers 1
```

Repeat with `--viewers 5` for higher fan-out. Collect app and DB CPU/memory with `docker stats` during the run. The control browser is one of the configured viewers. Other viewers consume real HTTP SSE streams. Browser timing includes the Playwright observer callback overhead. Add `--browser-stages` to also record paired SSE dispatch and DOM timestamps inside the control browser. See [the assessment](docs/results.md) and the raw output in `evidence/`.
