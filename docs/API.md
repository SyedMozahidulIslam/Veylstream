# API (`/api/v1`)
Envelope: `{"data":..., "meta":{...}, "request_id":"..."}`. Errors: `{"error":{"code","message","details"},"request_id"}`. Every response has `X-Request-ID` (echoed if you send one).
Auth: users `Authorization: Bearer <token>` (from `POST /auth/login`); producers `X-API-Key: vsk_...`. Workspace routes are `/api/v1/workspaces/<ws>/...`; non-members receive **404**. Roles: viewer < analyst < operator < admin.

| Area | Endpoints (min. role) |
|---|---|
| Health | `GET /health/live`, `/health/ready` (public), `GET /ingest/schema` (public) |
| Auth | `POST /auth/login` (10 tries/min/IP), `POST /auth/logout`, `GET /auth/me` |
| Ingest (API key) | `POST /ingest/events` (one), `/ingest/batch` (`{"events":[...]}`, max 1000), `/ingest/heartbeat` |
| Overview/streams | `GET /overview`, `/streams`, `/pipeline/status` (viewer); `POST /streams/processor/{pause,resume}` (operator) |
| Analytics | `GET /analytics/series?entity&metrics&from&to[&format=csv]`, `/analytics/entities`, `/analytics/breakdown?by=service\|region`, `/analytics/latency-distribution` |
| Anomalies | `GET /anomalies?severity&detector&entity&metric&min_severity&q&from&to&sort&page&page_size`, `GET /anomalies/<id>` |
| Alerts | `GET /alerts`, `GET /alerts/<id>`; operator: `POST /alerts/<id>/status`, `/assign`, `/notes`, `/alerts/bulk-status`, `POST /suppressions` |
| Data quality | `GET /dq/rules`, `/dq/findings`; operator `POST /dq/evaluate`; admin `POST/PATCH /dq/rules` |
| Events | `GET /events` (filters), `GET /events/<id>`; analyst `GET /dead-letters` |
| Sources | `GET /sources`; admin `POST /sources`, `/sources/<id>/rotate`, `/sources/<id>/revoke` (key shown once) |
| Notifications | `GET /notifications`, `POST /notifications/read`; admin: `/destinations` (CRUD, `/test`), `/deliveries` |
| Replay/lab | `GET /datasets`, `GET /replays`, `GET /experiments` (viewer); analyst: `POST /datasets/import?format=csv\|jsonl&name=`, `/datasets/synthetic`, `POST /replays`, `/replays/<id>/{pause,resume,stop}`, `GET /replays/compare?a=live&b=<id>`, `POST /experiments`, `GET /experiments/<id>/export?format=json\|csv\|md` |
| Settings | `GET /settings`; admin `PATCH /settings` (allow-listed, validated), `GET /audit` |
| Demo | `GET /demo`; operator `POST /demo/{seed,start,stop,trigger}`; admin `POST /demo/reset` |
| Live | `GET /live` (Server-Sent Events; `hello`, `stats` every 2 s, `window`, `anomaly`, `alert`; keep-alive; bounded queue). Metrics: `GET /metrics` (Prometheus text). |

Ingest status codes: `202` all accepted, `207` partly rejected, `422` all rejected, `400` empty, `401` bad key, `413` too large, `429` rate limited (`Retry-After`), `503` backpressure. Pagination: `page`, `page_size` ≤ 200; sort keys are allow-listed (`sort=-time`). Event contract: `GET /ingest/schema`.

```bash
curl -X POST localhost:8080/api/v1/ingest/events -H "X-API-Key: vsk_..." -H "Content-Type: application/json" \
 -d '{"event_id":"tx-1","event_type":"transaction","event_timestamp":"2026-10-26T10:00:00Z","service_name":"payments","region":"eu-west","status":"failed","amount":42.5,"currency":"USD","latency_ms":900}'
# {"data":{"accepted":1,"duplicates":0,"rejected":[],"durable":true,...},"meta":{"durability":"fsync"}}
```
