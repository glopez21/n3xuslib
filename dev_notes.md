# n3xuslib — Development Notes

**Version:** 0.1.0
**Stack:** Python 3.11+ · httpx · asyncpg

## Architecture

```
Daemon (logsentry/eventflow/alertflow)
    ↓
N3xusClient.emit()
    ├── http mode  → httpx POST → Omn1L1nk ingest API
    │                 └── 4xx/5xx/timeout → SQLite buffer (auto-flush)
    └── direct mode → asyncpg INSERT → n3xusDB.event_outbox
```

## Key Files

| File | Purpose |
|------|---------|
| `n3xuslib/client.py` | `N3xusClient` — `emit()` with http and direct transports |
| `n3xuslib/config.py` | `N3xusConfig` — env-driven config dataclass |
| `n3xuslib/buffer.py` | `SQLiteBuffer` — offline fallback with background flush |

## Transport Details

- **HTTP mode**: `base_url` from env, POSTs to `/api/v1/ingest`. Falls back to SQLite on any failure.
- **Direct mode**: Replaces `postgresql+asyncpg` prefix with `postgresql` for asyncpg compat, INSERTs into `event_outbox`.
- **Buffer**: SQLite at `~/.n3xuslib/outbox.db`, flushed every 30s, capped at 50 events per flush.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `N3XUSLIB_MODE` | `http` | `http` or `direct` |
| `N3XUSLIB_ENDPOINT` | `http://localhost:8100` | Omn1L1nk server URL |
| `N3XUSLIB_API_KEY` | — | API key for ingest auth |
| `N3XUSLIB_DB_URL` | — | PostgreSQL URL (direct mode) |
| `N3XUSLIB_BUFFER_PATH` | `~/.n3xuslib/outbox.db` | SQLite buffer path |
| `N3XUSLIB_SOURCE_INSTANCE` | `""` | Node/host identifier |

## Integration Points

- **LogSentry**: `n3xus.emit_alert()` — sync wrapper, called in `_send_alert()`
- **EventFlow**: `n3xus.emit_event()` — async, called after Redis publish in `EventManager.create_event()`
- **AlertFlow**: `AugurNotifier._emit_n3xus()` — sync, called in `push_triage_result()` finally block
