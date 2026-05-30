import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import httpx

from n3xuslib.config import N3xusConfig
from n3xuslib.buffer import SQLiteBuffer

logger = logging.getLogger("n3xuslib.client")


class N3xusClient:
    def __init__(self, config: N3xusConfig | None = None):
        self._cfg = config or N3xusConfig.from_env()
        self._http: httpx.AsyncClient | None = None
        self._buffer: SQLiteBuffer | None = None
        self._flush_task: asyncio.Task | None = None
        self._pool = None
        self._closed = False

    async def __aenter__(self):
        if self._cfg.mode == "http":
            self._http = httpx.AsyncClient(
                base_url=self._cfg.endpoint,
                timeout=15,
                headers={"X-API-Key": self._cfg.api_key or ""},
            )
            buf_path = self._cfg.buffer_path
            self._buffer = SQLiteBuffer(buf_path)
            self._flush_task = asyncio.create_task(self._flush_loop())
            logger.info(
                "n3xuslib http mode — endpoint=%s buffer=%s",
                self._cfg.endpoint,
                buf_path,
            )

        elif self._cfg.mode == "direct":
            import asyncpg

            dsn = self._cfg.db_url.replace("postgresql+asyncpg://", "postgresql://").replace(
                "+asyncpg", ""
            )
            self._pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
            logger.info("n3xuslib direct mode — db=%s", dsn)

        else:
            raise ValueError(f"unknown n3xuslib mode: {self._cfg.mode}")

        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        if self._closed:
            return
        self._closed = True

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        if self._http:
            await self._http.aclose()

        if self._pool:
            await self._pool.close()

        if self._buffer:
            self._buffer.close()

    async def emit(
        self,
        *,
        source: str,
        source_instance: str,
        event_type: str,
        severity: str = "low",
        title: str = "",
        payload: dict | None = None,
        context: dict | None = None,
        tags: list[str] | None = None,
        raw: str | None = None,
        created_at: str | None = None,
    ) -> str:
        if self._closed:
            raise RuntimeError("n3xuslib client is closed")

        event_id = str(uuid.uuid4())

        if self._cfg.mode == "direct":
            await self._direct_insert(
                event_id, source, source_instance, event_type,
                severity, title, payload, context, tags, raw, created_at,
            )
        else:
            await self._http_send(
                event_id, source, source_instance, event_type,
                severity, title, payload, context, tags, raw, created_at,
            )

        return event_id

    async def flush_buffer(self) -> int:
        if not self._buffer or self._cfg.mode != "http":
            return 0

        pending = await self._buffer.pop_pending(50)
        if not pending:
            return 0

        sent_ids = []
        for buf_id, event in pending:
            try:
                r = await self._http.post("", json=event, timeout=15)
                r.raise_for_status()
                sent_ids.append(buf_id)
            except Exception:
                logger.warning("failed to flush buffered event %s, will retry", buf_id)
                break

        if sent_ids:
            await self._buffer.mark_sent(sent_ids)

        return len(sent_ids)

    # ── Direct mode ────────────────────────────────────────

    async def _direct_insert(
        self, event_id, source, source_instance, event_type,
        severity, title, payload, context, tags, raw, created_at,
    ):
        async with self._pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO event_outbox (id, source, source_instance, event_type,
                       severity, title, payload, context, tags, raw, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::text[], $10, $11)""",
                uuid.UUID(event_id),
                source,
                source_instance,
                event_type,
                severity,
                title,
                json.dumps(payload or {}),
                json.dumps(context or {}),
                tags or [],
                raw,
                datetime.fromisoformat(created_at) if created_at else datetime.now(timezone.utc),
            )

    # ── HTTP mode ──────────────────────────────────────────

    async def _http_send(
        self, event_id, source, source_instance, event_type,
        severity, title, payload, context, tags, raw, created_at,
    ):
        body = {
            "source": source,
            "source_instance": source_instance,
            "event_type": event_type,
            "severity": severity,
            "title": title,
            "payload": payload or {},
            "context": context or {},
            "tags": tags or [],
            "raw": raw,
        }
        if created_at:
            body["created_at"] = created_at

        try:
            r = await self._http.post("", json=body, timeout=15)
            r.raise_for_status()
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.error("invalid API key — event dropped")
                return
            logger.warning("http ingest failed (%s) — buffering", e.response.status_code)
        except httpx.RequestError as e:
            logger.warning("omn1l1nk unreachable (%s) — buffering", e)

        await self._buffer.push(body)

    # ── Background flush loop ──────────────────────────────

    async def _flush_loop(self):
        while True:
            try:
                await asyncio.sleep(self._cfg.buffer_flush_interval)
                n = await self.flush_buffer()
                if n:
                    logger.info("flushed %d buffered events", n)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("buffer flush error")
