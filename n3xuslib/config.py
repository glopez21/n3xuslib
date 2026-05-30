import os
from dataclasses import dataclass, field


@dataclass
class N3xusConfig:
    mode: str = "http"
    db_url: str = "postgresql+asyncpg://omn1l1nk_user:changeme_omn1l1nk@localhost:5435/shared_db"
    endpoint: str = "http://localhost:8100"
    api_key: str | None = None
    buffer_path: str = field(default_factory=lambda: os.path.expanduser("~/.n3xuslib/outbox.db"))
    buffer_flush_interval: int = 30
    source: str = "unknown"
    source_instance: str = ""

    @classmethod
    def from_env(cls) -> "N3xusConfig":
        return cls(
            mode=os.environ.get("N3XUSLIB_MODE", "http"),
            db_url=os.environ.get("N3XUSLIB_DB_URL", cls.db_url),
            endpoint=os.environ.get("N3XUSLIB_ENDPOINT", cls.endpoint),
            api_key=os.environ.get("N3XUSLIB_API_KEY"),
            buffer_path=os.path.expanduser(
                os.environ.get("N3XUSLIB_BUFFER_PATH", cls.buffer_path)
            ),
            buffer_flush_interval=int(os.environ.get("N3XUSLIB_BUFFER_FLUSH_INTERVAL", "30")),
            source=os.environ.get("N3XUSLIB_SOURCE", "unknown"),
            source_instance=os.environ.get("N3XUSLIB_SOURCE_INSTANCE", ""),
        )
