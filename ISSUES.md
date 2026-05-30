# n3xuslib — Known Issues

## Resolved

- `cls.buffer_path` field access crash — `field(default_factory=...)` can't be accessed on class. Fixed by using plain string default.
- HTTP double-path bug — `base_url` included `/api/v1/ingest` but code also appended `/api/v1/ingest`, resulting in `//api/v1/ingest/api/v1/ingest`. Fixed by using server-only base URL.

## Known

- No retry with backoff on buffer flush — retries immediately on interval
- SQLite buffer has no size/memory cap — unbounded growth if server is down for days
- No TLS/HTTPS support yet
- `httpx.TimeoutException` caught by `httpx.RequestError` base class (not an issue, just broad)
