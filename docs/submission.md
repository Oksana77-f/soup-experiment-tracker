# Soup Experiment Tracker · Submission note

**SHIP for the measured local profile.** One app instance, one viewer per run, 50 concurrent runs at 10 points per run per second, and a 60 s workload. Both runs delivered all 30,000 points without duplicates. Control-browser p99 was 1.61–1.68 s against a 2 s gate, leaving 0.32–0.39 s of headroom. Two short runs provide limited evidence about stability over time. [Benchmark revision `aee59f3`](https://github.com/Oksana77-f/soup-experiment-tracker/tree/aee59f3) · [Baseline runs](results.md#measured-load)

## What the latency measures

Both timestamps use `perf_counter()` in the same Python process on WSL2. The driver starts the timer after the scheduled wait, just before building JSON and sending the POST. Chromium’s `MutationObserver` reports a new row through Playwright, and the driver timestamps receipt of that callback. The interval includes the callback bridge. The control page tracks 600 rows per minute. Nearest-rank p99 is the 594th sample in sorted order. [Load driver](../scripts/load.py)

The 1 s polling interval is a sleep after an empty read. In the repeat, POST-to-acknowledgment p99 was 0.64 s. HTTP SSE p99 of 1.20 s and browser DOM p99 of 1.61 s describe different samples. A separate in-page diagnostic paired 300 events and measured 6.2 ms p99 from SSE dispatch to DOM observation. The Playwright callback bridge remains part of the end-to-end number. [Baseline](../evidence/step14-summary.json) · [Paired diagnostic](../evidence/browser-stage.txt)

**DON’T SHIP at five viewers per run.** The second profile delivered all 150,000 expected updates. DOM p99 rose to 2.65 s, driver POST p99 to 1.86 s and HTTP SSE p99 to 2.25 s. Sampled peak app CPU rose from 37.80% to 77.43% and database CPU from 22.02% to 33.55%. Write-pool wait p99 fell from 80.68 to 70.57 ms. [Five-viewer summary](../evidence/step15-summary.json)

**Measured extra work.** In two 30 s diagnostics, PostgreSQL page reads rose from 3,044 to 14,607 (4.8×), with mean execution near 0.14 ms per query. The application CPU increase also warrants measuring event-loop delay and SSE serialization and sends. The read count establishes repeated work, while its share of the DOM tail remains unmeasured. [SQL diagnostic](../evidence/fanout-sql.txt)

**First change.** Measure event-loop lag and SSE read and send durations under both viewer counts. A shared live reader per run is the candidate if repeat reads account for a material share. PostgreSQL replay and per-viewer authorization remain part of that design.

## Consistency and isolation

The Chromium test combines duplicate frames, repeated batches, late points and a controlled disconnect. It compares the sent values, PostgreSQL and the final DOM. Another variant injects a `502` on reconnect. The recovery path reads persisted events after the last applied `seq`. [Browser tests](../tests/test_stream.py)

Separate keys restrict reading and writing to the owning project. Tests cover the run list, snapshot, write, SSE and returning to the previous project’s URL through browser history. Foreign and unknown IDs have the same read and SSE error body. Protected responses use `Cache-Control: no-store`. [API tests](../tests/test_api.py) · [Browser tests](../tests/test_stream.py)

**Other leak paths.** The database error handler returns generic text and logs the error class and SQLSTATE. Proxy or trace logs can expose headers or payloads if configured to record them. Identical error bodies still leave timing side channels possible. [Isolation design](design.md)

<!-- pagebreak -->

## Silent failures and how they were checked

Both Compose images built with `--no-cache`. The app served the tracker UI and all 18 tests passed on the new test image. [Clean build and test output](../evidence/nocache-check.txt)

| Failure | How it was checked |
| --- | --- |
| Events lost on reconnect | A controlled SSE disconnect holds the next request while the writer adds points. The final DOM matches input and PostgreSQL. Disabling replay leaves missing rows. [Expected failure](../evidence/red-replay.txt) |
| Duplicates | A batch is retried with the same ID and then with a new ID. A browser variant receives a duplicate SSE frame. Two concurrent batches sharing a point both succeed. Removing the run lock produces a uniqueness error. [Expected failure](../evidence/red-concurrent-write.txt) |
| Slow client and queue overflow | The stream reads up to 100 points at a time without a growing subscriber queue. The test stalls an ASGI data send and checks the 3 s timeout, release of the DB connection and replay after reconnect. [Stream test](../tests/test_stream.py) |
| Crash around `COMMIT` | `SIGKILL` drops the real HTTP connection in both tests. Before `COMMIT`, the run and receipt are empty and retry inserts two points. After `COMMIT` and before the response, retry returns the stored acknowledgment with the same `batch_id`. The two points and cursor stay the same. [Crash tests](../tests/test_crash.py) |
| Old key after rotation | The old write key and read session are rejected after rotation. The idle stream rechecks after up to 1 s of sleep plus DB and scheduling time. The clean run observed EOF 0.948 s after the rotation commit, within its 2 s gate. A fetched page or an authorized write may already be in flight. [Rotation test](../tests/test_api.py) |

**What the tests can miss.** Multi-instance deployments, database failover, clock skew between hosts and other browsers are outside the setup. Controlled disconnect and stalled-send hooks can miss real TCP backpressure, keepalive stalls and bugs in the hooks themselves. Browser history traversal does not exercise an in-memory page restore or a misconfigured shared cache. The one-clock latency calculation avoids cross-host clock subtraction and does not model clock skew in production.

**Unexpected result and remaining concern.** An SSE shutdown flag carried over from one in-process test server to the next. Later streams closed as soon as they opened. Resetting the flag between fixtures fixed the test isolation. The five-viewer run still exceeded the latency gate even with every point delivered. The early failed load runs are preserved. [Full report](results.md)

**AI use.** Codex assisted with implementation, tests, load analysis and editing. The candidate identified the commit-order risk of a PostgreSQL sequence, the need for a deterministic race barrier and the failure mode after an SSE `502`. The resulting changes were checked with PostgreSQL and Chromium, including the two intentionally failing controls. [AI-use log](ai-usage-ru.md)
