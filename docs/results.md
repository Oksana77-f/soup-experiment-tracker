# Experiment tracker assessment

**SHIP for the measured local profile with one observer per run. DON’T SHIP for five observers per run.** The integrated runtime measured DOM p99 of 1.525 s with one observer and 3.503 s with five. The two-worker diagnostic at five reached 2.216 s. The gate requires p99 below 2 s, zero losses or duplicates and a writer that sustains the schedule. Earlier runs are preserved below.

## Measured load

PostgreSQL 17.11 and one Uvicorn process ran through Docker Compose on WSL2 with 12 logical CPUs and 5.8 GiB visible to Docker. Each of 50 runs issued one ten-point batch per second. The base runs had one observer per run and step 15 had five. One Chromium instance measured DOM updates. The other 49 or 249 observers consumed HTTP SSE. The runs lasted 60 seconds and leave long-run degradation unmeasured.

The driver stamps a batch with `perf_counter()` after the scheduled wait, immediately before serializing it and starting the first POST. The same Python process in WSL2 timestamps receipt of the callback sent by Chromium’s mutation observer. No browser or container timestamp is subtracted from the Python clock. The 600 DOM observations in each minute use the 594th sample in sorted order. The interval includes the observer callback bridge. The HTTP SSE p99 combines other subscribers. The difference between their p99 and the control browser’s p99 cannot identify time spent in React or Playwright. [Benchmark revision `aee59f3`](https://github.com/Oksana77-f/soup-experiment-tracker/tree/aee59f3) · [Load driver](../scripts/load.py)

**Paired browser diagnostic.** With `--seconds 30 --viewers 1 --browser-stages`, an early `EventSource` listener and the DOM mutation observer both used Chromium `performance.now()` for each point in the control browser. Of 300 paired events, p50 was 1.2 ms, p95 was 3.4 ms and p99 was 6.2 ms. That interval begins at browser event dispatch and ends in the observer before calling Playwright. It excludes browser network reception and the callback bridge to Python. A test-image dependency download was running in the background, so this is a short stage diagnostic. The driver’s end-to-end p99 in this run was 1.57 s. The paired measurement provides no estimate of how much time the callback bridge adds. [Diagnostic output](../evidence/browser-stage.txt)

| Run | Stored / attempted | Write window | POST p99 | SSE p99 | DOM p50 / p99 |
|---|---:|---:|---:|---:|---:|
| [First 60 s](../evidence/load-first.txt) | 30,000 / 30,000 | 75.62 s | 5.38 s | 5.84 s | 3.02 / 8.04 s |
| [Separate 60 s, without retries](../evidence/load-no-retry.txt) | 29,990 / 30,000 | 59.61 s | 1.46 s | 2.30 s | 0.68 / 2.49 s |
| [30 s, sender retry policy enabled](../evidence/load-1-viewers.txt) | 15,000 / 15,000 | 29.72 s | 0.74 s | 1.61 s | 0.61 / 1.64 s |
| [60 s, step 12 with POST timings](../evidence/step12-load.txt) | 30,000 / 30,000 | 59.62 s | 0.64 s | 1.21 s | 0.58 / 1.68 s |
| [60 s, step 14 confirmation](../evidence/step14-load.txt) | 30,000 / 30,000 | 59.61 s | 0.64 s | 1.20 s | 0.63 / 1.61 s |
| [60 s, step 15 with five observers per run](../evidence/step15-load.txt) | 30,000 / 30,000 | 60.44 s | 1.86 s | 2.25 s | 1.26 / 2.65 s |

The first run overlapped a test suite and exceeded the write schedule. The separate one-minute run reused HTTP connections and recorded one unacknowledged POST after `RemoteProtocolError`. PostgreSQL was ten points short. The old subscriber counter compared full tuples, including shifted sequence numbers, so its 360 figure describes missing expected tuples. The revised harness counts missing metric values independently and still checks the complete ordered sequence. The 30-second control reported zero missing or duplicate points, zero errors and zero retry attempts. In that control the highest sampled app and database CPU readings were 38.57% and 17.81%, with peak sampled memory of 64.26 MiB and 87.23 MiB. [Resource samples](../evidence/load-1-viewers-resources.txt)

**Step 12 conditions.** The retry-enabled driver ran as a separate WSL process on the same machine as Compose, with the POST timing log enabled and no concurrent tests. The run had zero retry attempts, losses, duplicates or database mismatches. [Environment](../evidence/step12-environment.txt) · [Resources](../evidence/step12-resources.txt)

**Step 13 diagnosis.** Across 3,000 accepted POSTs, server p99 was 169.35 ms. The write-pool acquisition p99 was 95.40 ms, the locking query p99 was 2.28 ms and commit p99 was 7.51 ms. Pool acquisition was the largest measured component in all 30 slowest handlers, with a median share of 66%. All 30 were in the first 50 completed POSTs. After those first 50, pool acquisition p99 was 0.08 ms and handler p99 was 29.29 ms. This is a startup concentration in the observed run. The pass gate did not trigger a corrective change. [Summary and tail analysis](../evidence/step12-summary.json) · [Raw POST timings](../evidence/step12-post.txt)

**Step 14 confirmation.** The repeat used the same app image, PostgreSQL version, retry-enabled driver, Chromium and subscriber profile. It recorded zero losses, duplicates, mismatches, errors or retries across 3,000 POSTs. The driver was a separate WSL process on the same physical machine. The release decision covers this one-observer profile. [Environment](../evidence/step14-environment.txt) · [Summary](../evidence/step14-summary.json) · [Raw POST timings](../evidence/step14-post.txt)

**Step 15 fan-out.** With 250 subscriptions, all 150,000 expected deliveries matched the input and database. There were zero errors or retries. The write window stayed within the 61 s gate. DOM p99 exceeded the 2 s gate. Sampled peak app CPU rose from 37.80% to 77.43%, and PostgreSQL CPU from 22.02% to 33.55%. Peak sampled memory was 75.06 MiB for the app and 106.40 MiB for PostgreSQL. Server handler p99 was 153.38 ms versus 174.35 ms at step 14, while write-pool p99 was 70.57 ms versus 80.68 ms. The generator and services shared a host. The aggregate timings describe the slowdown under fan-out and leave its exact source unresolved. [Summary](../evidence/step15-summary.json) · [Resources](../evidence/step15-resources.txt) · [Raw POST timings](../evidence/step15-post.txt)

**SQL read diagnostic.** Two separate 30 s workloads used the same Compose app and DB, with POST timing logs off and `pg_stat_statements` enabled on the isolated database. The counters include startup reads, the snapshot and the final drain. Page queries increased from 3,044 to 14,607 with five viewers, and their total execution time increased from 435.65 ms to 1,895.96 ms. Mean execution time per query was 0.143 ms and 0.130 ms. SQL execution timing excludes connection-pool wait, app scheduling and client time. The five-viewer diagnostic also missed the write-window gate (32.306 s against 31 s), with all 15,000 points and all deliveries intact. The read count justifies testing a shared live reader per run. An A/B run is needed to quantify its effect on DOM p99. [Diagnostic output](../evidence/fanout-sql.txt)

To reproduce the counters, start a separate Compose database with `shared_preload_libraries=pg_stat_statements`, run `CREATE EXTENSION pg_stat_statements` in `soup`, and reset the counters before each `--seconds 30` workload. Read `calls` and `total_exec_time` for the query whose normalized SQL starts with `SELECT r.id, r.name, r.last_seq, page.points`.

## Integrated runtime measurement

Revision `92c13ae` includes session revocation and timeouts on every SSE send. The two workloads used identical app images, fresh databases, one Uvicorn worker, one control Chromium and real HTTP SSE for all remaining viewers. The 60 s workloads on the same WSL2 host used both timing flags. [Summary](../evidence/criteria-load/summary.json) · [One viewer](../evidence/criteria-load/viewers-1-driver.txt) · [Five viewers](../evidence/criteria-load/viewers-5-driver.txt)

| Metric | One viewer per run | Five viewers per run |
| --- | ---: | ---: |
| Stored points / deliveries | 30,000 / 30,000 | 30,000 / 150,000 |
| POST to DOM p99 | 1.525 s | 3.503 s |
| Driver POST p99 | 597 ms | 2,083 ms |
| App POST handler p99 | 164 ms | 240 ms |
| App loop delay p99 upper bucket | 3 ms | 8 ms |
| App page read p99 upper bucket | 12 ms | 48 ms |
| ASGI data send p99 upper bucket | 0.2 ms | 0.2 ms |
| App SSE page calls | 6,044 | 28,192 |
| Driver schedule lag p99 | 110 ms | 2,147 ms |
| Sampled peak app / DB CPU | 44.78% / 20.41% | 82.25% / 36.98% |

Both workloads completed with zero losses, duplicates or mismatches. The single-viewer run recovered one transport retry. The five-viewer tail persisted beyond the first 50 completed writes, and warm write pool p99 increased from 0.078 ms to 42.1 ms. The ASGI send timing ends when the server accepts the frame and the read timing includes pool acquisition. Driver schedule lag precedes the end-to-end clock. These aggregated distributions show pressure on the app, pool and driver without decomposing a specific slow event. The first diagnostic change is to move the non-browser SSE consumers out of the timing driver and repeat the same 250 connections before changing polling or pooling. [One-viewer app timing](../evidence/criteria-load/viewers-1-app.txt) · [Five-viewer app timing](../evidence/criteria-load/viewers-5-app.txt)

**Two-worker diagnostic.** The same image ran with two Uvicorn workers, a shared signing secret provisioned before startup and two independent DB pools. At five viewers, DOM p99 fell from 3.503 to 2.216 s. App handler p99 fell from 240 to 163 ms and warm write pool p99 from 42.1 to 0.095 ms. App memory peaked at 121.7 MiB against 65.16 MiB for one worker. The trial retained all 30,000 stored points and 150,000 deliveries. PostgreSQL connections reached 32, so the result reflects both workers and greater pool capacity. This single run remains above the latency gate. [Driver](../evidence/criteria-load/two-workers/viewers-5-driver.txt) · [Summary](../evidence/criteria-load/two-workers/viewers-5-summary.json) · [Environment and connections](../evidence/criteria-load/two-workers/viewers-5-environment.txt)

## Correctness and boundaries

The original WSL pytest suite passed all 11 checks against PostgreSQL and Chromium. It compares input, database and DOM after a held reconnect and a separate `502`, checks project isolation and browser reuse, exercises batch conflicts and rollback, and checks rotation, restart and a stalled ASGI send of a data frame. Keepalive sends are outside that test. The app also passed a real Docker Compose restart smoke test. [Original pytest run](../evidence/pytest-final.txt) · [16 checks after POST timing](../evidence/pytest-step11.txt) · [Container restart](../evidence/compose-restart.txt)

The replay mutation left one row visible where three were expected. The lock mutation produced HTTP `200` and `500` with SQLSTATE `23505` where two successes were required. [Replay control](../evidence/red-replay.txt) · [Lock control](../evidence/red-concurrent-write.txt)

The earlier 17-test Compose run killed a separate writer before commit. With the default 1 s idle poll, the rotation test observed the old stream end 0.946 s after the rotation commit, within its 2 s test deadline. This is a local observation, and in-flight pages or already authorized writes can finish. The rebuild hit a PyPI download timeout, so that run used the prior fresh-clone test image with the then-current tests mounted read-only. [17-test output](../evidence/submission-checks.txt) · [Build timeout](../evidence/submission-build-timeout.txt) · [Retry command output](../evidence/submission-reuse-cli.txt)

Both crash cases now go through a separate HTTP worker. `SIGKILL` before commit drops the connection and leaves zero points and no receipt. The exact retry writes two points. After commit, the same connection loss leaves two points and the original receipt. The retry returns that exact receipt and leaves the snapshot unchanged. [Crash tests](../tests/test_crash.py) · [18-test preflight](../evidence/post-commit-preflight.txt)

The clean verification ran `docker compose --profile test build --no-cache app test` with an isolated Compose project and a new database. The app served the tracker UI and all 18 tests passed in 17.70 s on the newly built test image. The rotation test observed EOF in 0.948 s. Dependency downloads completed in this run. [Clean build and test output](../evidence/nocache-check.txt)

The integrated verification used a current app build and mounted the current source, tests and built React UI read-only in the existing test dependency image. All 29 cases passed. New coverage includes copied-cookie revocation after restart, logout in other tabs, writes between snapshot and first subscription, replay across more than two production pages, and blocked data, heartbeat and final sends. A Linux loopback TCP socket with constrained buffers stopped reading and triggered the deadline. A new reading client then replayed all 2,048 events in order. Chromium reported that history traversal did not restore from BFCache. The raw test log retains the timeout traceback and an unlabelled incomplete-ASGI-response line. [Integrated output](../evidence/criteria-final.txt) · [Contract regressions before the fix](../evidence/contract-logout-red.txt) · [TCP regression](../evidence/tcp-regression.txt)

The browser scenarios cover one engine and controlled HTTP disconnects. Database failover, long histories, fan-out above five observers per run, intermediate cache misconfiguration and timing side channels remain outside these measurements. Logs record error classes and SQLSTATE rather than request credentials or metric bodies. The demo sender keeps an outstanding batch in memory during retries. PostgreSQL and application-state volumes survive the app restart tested here.

The surprising validation failure was an SSE shutdown flag shared across in-process test servers. It caused later fixtures to terminate streams on startup. Resetting that fixture state and using the configured send timeout restored a passing full suite. The remaining concern is the write tail at sustained load. Codex assisted with code, tests, documentation and the measurement runs. [AI-use log](ai-usage-ru.md)
