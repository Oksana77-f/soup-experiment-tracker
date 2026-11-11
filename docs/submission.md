# Soup Experiment Tracker · Submission note

**SHIP for the measured local profile.** One Uvicorn worker, 50 concurrent runs at 10 points per run per second, one viewer per run and a 60 s workload. All 30,000 points arrived without losses or duplicates. POST to DOM p99 was 1.525 s against a 2 s gate. The short run leaves long-run degradation unmeasured. [Runtime revision `92c13ae`](https://github.com/Oksana77-f/soup-experiment-tracker/tree/92c13ae) · [Raw result](../evidence/criteria-load/viewers-1-driver.txt)

## Load verdict and first change

The driver uses `perf_counter()` in one WSL2 Python process before preparing the POST and upon receiving the Chromium DOM observer callback. One browser supplies 600 samples. Nearest-rank p99 is the 594th value in sorted order. Other viewers use real HTTP SSE. A separate paired in-page probe measured 6.2 ms p99 from SSE dispatch to DOM observation on 300 points. The end-to-end measurement includes the Playwright callback bridge. [Driver](../scripts/load.py) · [In-page probe](../evidence/browser-stage.txt)

**DON’T SHIP at five viewers per run.** One worker delivered all 150,000 updates, with DOM p99 of 3.503 s. Going from one to five viewers raised page calls from 6,044 to 28,192 and the read p99 upper bucket from 12 to 48 ms. App loop delay rose from ≤3 to ≤8 ms and ASGI data send stayed at ≤0.2 ms. These phase aggregates measure different samples. [Raw result](../evidence/criteria-load/viewers-5-driver.txt) · [Phase summary](../evidence/criteria-load/summary.json)

**Two-worker diagnostic.** At five viewers the same image delivered all updates with p99 of 2.216 s, still above the gate. The two workers opened 32 DB connections by the end, so the result combines extra workers and pool capacity. [Trial evidence](../evidence/criteria-load/two-workers/viewers-5-summary.json)

**First change.** Move background SSE viewers out of the timing driver and repeat the same workload. Driver schedule lag grew from 110 to 2,147 ms p99 and precedes the measured interval. An independent driver helps distinguish load generation effects from application and database pressure before changing the polling design. [Full analysis](results.md#integrated-runtime-measurement)

## Consistency and isolation

The Chromium test compares sent values, PostgreSQL and the final DOM after duplicates, late points and a disconnect, with a separate `502` variant. An additional test writes 101 points between snapshot and first subscription. Reconnect also drains more than two production pages and then receives a new live point. [Browser tests](../tests/test_stream.py) · [Replay boundaries](../tests/test_replay_boundaries.py)

Foreign and unknown run IDs return identical `404` responses on tested read and write routes. Protected responses use `Cache-Control: no-store`. Logout revokes one session in PostgreSQL, rejects its copied cookie after restart and closes its open stream. Other tabs clear the previous project while independent sessions remain valid. Chromium Back navigation preserved isolation and did not restore the page from BFCache. [API tests](../tests/test_api.py) · [Browser evidence](../evidence/criteria-final.txt)

**Other leak paths.** Database errors return generic text and logs record the class and SQLSTATE. Proxy logs, traces and response timing remain possible disclosure paths. [Isolation design](design.md)

<!-- pagebreak -->

## Silent failures and how they were checked

The integrated run passed all 29 tests with the current source and built UI on PostgreSQL and Chromium. It reused the dependency image from the previous `--no-cache` build. [Integrated output](../evidence/criteria-final.txt) · [Earlier clean build](../evidence/nocache-check.txt)

| Failure | How it was checked |
| --- | --- |
| Events lost on reconnect | A controlled SSE disconnect holds the next request while writes continue. The final DOM matches input and PostgreSQL. Disabling replay leaves missing rows. A real HTTP reconnect resumes from `Last-Event-ID` through multiple pages. [Expected failure](../evidence/red-replay.txt) |
| Duplicates | Repeated POST batches and SSE frames leave one copy of each point. Two concurrent batches share a point. Removing the run lock produces a uniqueness error. [Expected failure](../evidence/red-concurrent-write.txt) |
| Slow client and queue overflow | The app releases the DB connection before sending and reads at most 100 points per page. Tests stall data, heartbeat and final ASGI sends. A non-reading Linux TCP client reaches the timeout and a fresh subscriber replays all 2,048 ordered events. [TCP test](../tests/test_tcp_backpressure.py) |
| Crash around `COMMIT` | `SIGKILL` drops the HTTP response. Before commit the run and receipt are empty. After commit a retry of the exact batch returns the stored acknowledgment and leaves both points and cursor unchanged. [Crash tests](../tests/test_crash.py) |
| Old key after rotation | Old read and write keys fail and an idle SSE closes on the next authorization check. A frame already being sent or a write authorized before rotation can finish. An open stream on another worker observes logout at its next DB read. [Rotation and logout tests](../tests/test_api.py) |

**What the tests can miss.** Multiple hosts, database failover, clock skew, other browser engines, restored BFCache pages and shared proxies are outside the test setup. Loopback with constrained buffers covers one kind of TCP backpressure. Long histories still load the full snapshot. A two-worker load trial covers one process topology, with no broader availability claim.

**Unexpected result and remaining concern.** The library timed out blocked data sends while the heartbeat and final body followed separate paths. A common ASGI deadline and a non-reading socket test now cover these cases. The five-viewer tail still crosses the gate. Early failed runs and negative controls remain available. [Full report](results.md)

**AI use.** Codex and teammates helped implement changes, design negative controls, analyze measurements and edit the notes. The candidate reviewed assumptions about commit order, retries and recovery and ran the live demo locally. The log records the decisions and the checks used to verify them. [AI-use log](ai-usage-ru.md)
