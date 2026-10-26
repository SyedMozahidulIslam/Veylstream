# Implementation report, limitations and production readiness
Everything below was checked against the code and the runs listed here. Anything not listed as executed was **not** executed.

## 1. Executed results
| Check | Result |
|---|---|
| `python -m unittest discover -s tests` | 70 collected, **69 passed, 1 skipped** (opt-in slow test), 0 failed. ~39 s on 1 CPU. Unclosed-file `ResourceWarning`s from test fixtures appear (tests do not close every broker); they are test hygiene, not product failures. |
| `VEYLSTREAM_SLOW=1 ... -p test_e2e_slow.py` | **Passed** (~90 s): seed 30 min simulated history (0 anomalies/alerts on clean data) → live stream → `failure_spike` → alert opens (txn_failure_rate, ~1 min) → ack → resolve → synthetic dataset → replay → experiment. |
| Real HTTP smoke via CLI | `setup-demo`, `serve`, login, create source, SDK `send_batch` of 50 events → `accepted=50, durable=true`, 50 events stored. |
| `node --check` on `app.js`, `pages.js` | Syntax OK. |
| `python -m compileall` | OK. |
| **Not run (tool absent):** ruff, mypy, pyflakes, pip-audit, Docker build, docker compose, GitHub Actions, any browser, Playwright, load test beyond below. |

Measured on this sandbox (1 vCPU, Python 3.12.3, numpy 2.4.4; `experiments/results/pipeline.json`, 60,000 events, in-process, no HTTP hop): ingest ack throughput **~30.7k events/s with fsync per 500-event batch** (batch ack p50 15 ms, p95 24 ms), ~35.8k without fsync; stream processing **~18.4k events/s** end to end (aggregate + persist + detect); detection ≈ 0.15 ms per closed window for the default rule+MAD+z-score set. No claim is made about multi-core, multi-node, HTTP-inclusive, or sustained-hours capacity. Detector accuracy figures are in docs/RESEARCH.md.

## 2. Spec coverage: what exists versus what does not
**Implemented and tested:** versioned event contract and validation (transaction, telemetry, heartbeat); dead-lettering with reasons; idempotent dedupe; durable ack policy; HTTP ingest (single/batch/heartbeat, API-key auth, per-source rate limit, size limits, backpressure); Python SDK (retry with exponential backoff + jitter, no retry on permanent errors); event-time tumbling/sliding windows, watermark, late policy, empty windows, exact percentiles, bounded state, restart-equivalent recovery; detectors (rules with warn/critical + min support, z-score, MAD, IQR, baseline deviation, rate-of-change, seasonal baseline, Isolation Forest, ensemble); explainable detections with evidence and deterministic investigation steps; alert lifecycle (streak/grouping/escalation/cooldown/reopen/suppression/assignment/notes/audit, invalid transitions rejected); in-app notifications, webhooks (timeouts, bounded retries, delivery log, SSRF guard, real test action), SMTP e-mail path; data-quality rules (rejection/duplicate/late rates, reason counts, producer inactivity, freshness); pipeline health from real checks; CSV/JSONL import with reject reporting; replay (speed, pause/resume/stop, offset, isolated scope, provenance); experiments (labeled/unlabeled, reproducibility record, export); synthetic generator (seeded, 9 scenario kinds incl. duplicates/late/invalid/outage); RBAC with workspace isolation; hashed credentials; SSE live updates; demo seed/reset; UI pages for all 12 navigation areas plus a landing/login screen.

**Not implemented (these were in the brief):**
* **FastAPI, Pydantic, SQLAlchemy, Alembic, React/TypeScript/Vite/Tailwind/shadcn/TanStack/ECharts** (Flask, SQLite SQL migrations and a vanilla-JS UI were used instead; see ADR-001).
* **PostgreSQL/TimescaleDB, Redis, Parquet (import returns an explicit error without `pyarrow`), S3 storage.**
* **Redpanda/Kafka broker and Spark Structured Streaming: no adapter exists, no distributed mode, no consumer-group lag** (lag = unprocessed records in the local log). The interfaces (`Broker`, `StreamProcessor`) exist; the adapters and `docker-compose.distributed.yml` do not.
* Docker Compose multi-service setup, Grafana configuration, structured JSON logging, OpenTelemetry/trace propagation (correlation ids are stored and searchable; request ids are returned).
* User management beyond the CLI (`setup-demo`) and member listing: no invite/role-change/password-change endpoints or UI; no MFA/SSO; no per-user preferences beyond browser theme/time display.
* Detector configuration is stored as workspace settings, not as versioned `detector_definitions` tables; the active config is recorded inside each detection and experiment.
* Source-level start/stop of producers (only the processor can be paused/resumed); alert auto-resolve; escalation policies/on-call routing.
* Feature-level frontend tests, browser E2E, accessibility audit, responsive-layout verification, light-theme visual review. **The UI has never been rendered in a browser by the build**; expect rough edges.
* Performance tests for burst/sustained/backpressure were not run as scripts (backpressure and rate limiting are unit-tested; throughput benchmark exists).

## 3. Known weaknesses
* Isolation Forest was weak on the synthetic benchmark (F1 ≈ 0.21) and untuned. 10 s windows are noisy for rate metrics (observed false positives); 30 s is the default.
* `rate_of_change` and `baseline_deviation` are noisy; available but excluded from default sets.
* Deviation start time in the anomaly view is approximated from the earliest same-series detection within 10 windows.
* Percentile *breakdowns* by service/region average per-window p95 (labeled so); only per-window and the latency-distribution histogram (latest 20,000 events) are true percentiles.
* Ingest dedupe is a bounded LRU: a duplicate arriving after 200k newer ids is not detected at ingest (the events table still rejects it via primary key).
* A replay job interrupted by a server restart is not resumed. Failed webhook deliveries after retries are logged, not re-queued across restarts.
* Single-process Werkzeug server; the in-memory login limiter and SSE bus are per process.

## 4. Security posture (self-assessed; no external review)
Done: scrypt password hashing; opaque session tokens and API keys stored as SHA-256 only, shown once; no default account (random one-time admin password); role checks on every workspace route; non-members get 404; parameterised SQL everywhere with allow-listed sort columns (injection test included); input validation and size limits; login throttling (10/min/IP, in-memory); API key rotation/revocation; webhook SSRF guard (blocks private/loopback/link-local unless explicitly enabled); import file paths are server-generated and display names sanitised; UI escapes all dynamic text, CSP blocks inline scripts and third-party origins; `X-Frame-Options`, `nosniff`; secrets are not logged; sensitive actions are audited; entity ids masked for viewers in event detail.
Gaps: no TLS (use a proxy); bearer token stored in `sessionStorage` (XSS would expose it); no CSRF concern for bearer headers but no CSP nonce hardening of `style-src 'unsafe-inline'`; SMTP password must come from an environment variable; SQLite file is unencrypted; no rate limit on authenticated read endpoints; no dependency audit run.

## 5. Production readiness verdict
**Not production-ready.** It is a verified single-node local system suitable for demos, evaluation, and research on small to moderate event rates. Before real use you would need: the distributed storage/broker/processing tier, TLS and a real server, a browser-tested frontend, user management, load and soak tests, backup/restore drills, an external security review, and a Postgres migration. Reliability claims are limited to: crash-restart equivalence (tested), torn-write recovery (tested), bounded queues/backpressure (tested).

## 6. Recommended next steps (priority order)
1. Open the UI in a browser and fix whatever the first real session reveals; add Playwright for the journey in `test_e2e_slow.py`.
2. Install ruff/mypy and make CI green; run pip-audit.
3. Implement `PostgresStore` (same SQL) and a Redpanda `Broker` adapter; run the existing restart-equivalence test against them.
4. Port the UI to React/TS only if the type-safety/ecosystem is worth the rewrite; the API is stable and documented.
5. Tune/evaluate Isolation Forest on more varied and (ideally) real labeled data; add confidence intervals over more seeds.
