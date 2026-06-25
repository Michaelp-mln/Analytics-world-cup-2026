"""Pool asyncpg com retry de conexão no startup."""
from __future__ import annotations

import asyncio
import logging

import asyncpg

from wc2026.config import settings

log = logging.getLogger("wc2026.db")


async def make_pool(retries: int = 30, delay: float = 2.0) -> asyncpg.Pool:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            pool = await asyncpg.create_pool(
                dsn=settings.postgres_dsn, min_size=1, max_size=10,
            )
            log.info("Pool Postgres criado")
            return pool
        except (OSError, asyncpg.PostgresError) as exc:
            last_err = exc
            log.warning("Postgres indisponível (tentativa %d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(delay)
    raise RuntimeError(f"Não foi possível conectar ao Postgres: {last_err}")
