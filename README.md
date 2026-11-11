# Soup Experiment Tracker

A small, project-scoped tracker for batched training metrics. The browser reads a PostgreSQL snapshot and receives committed metrics over SSE. See [the submission note](docs/submission.md), [the design](docs/design.md), [the working plan in Russian](docs/plan-ru.md) and [the AI-use log](docs/ai-usage-ru.md).

## Run the demo

```sh
docker compose up -d --build --wait
docker compose exec app python -m tracker.cli demo
```

Open [localhost:8000](http://127.0.0.1:8000). Sign in with one project's `read_key` and open its training run. Run the sender using that project's `write_key` and `run_id` from the CLI output.

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

## Verify

```sh
docker compose --profile test run --rm --build test
docker compose --profile test run --rm -e TRACKER_TEST_MUTATION=skip-replay test \
  python -m pytest -q 'tests/test_stream.py::test_browser_matches_input_and_database_after_disconnect[native-reconnect]'
docker compose --profile test run --rm -e TRACKER_TEST_MUTATION=no-run-lock test \
  python -m pytest -q tests/test_api.py::test_concurrent_batches_share_one_point
```

The last two commands should fail. They change behavior only inside their test fixtures. The test service uses a separate `soup_tests` database. The browser checks compare actual DOM rows, stored data and input. The deterministic network test closes an HTTP SSE response from the server, then holds the next request while it writes new points. A separate variant returns one `502`. The slow-client test stalls an ASGI send of a data frame.

The [fresh-clone check](evidence/fresh-clone-compose.txt) records the browser demo. The [latest full Compose run](evidence/submission-checks.txt) includes the writer process kill before commit and rotation at the default idle poll interval.

Run the benchmark on a local instance with a disposable demo database. It creates a separate benchmark project for every invocation.

```sh
docker compose --profile test run --rm \
  -e DATABASE_URL=postgresql://soup:local-compose-only@db:5432/soup \
  test python -m scripts.load --url http://app:8000 --seconds 60 --viewers 1
```

Repeat with `--viewers 5` for higher fan-out. Collect app and DB CPU/memory with `docker stats` during the run. The control browser is one of the configured viewers. Other viewers consume real HTTP SSE streams. Browser timing includes the Playwright observer callback overhead. See [the assessment](docs/results.md) and the raw output in `evidence/`.
