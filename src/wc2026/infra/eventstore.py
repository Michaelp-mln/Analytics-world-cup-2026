"""Acesso ao EVENT STORE (append-only).

A versão (`version`) de cada evento é atribuída no momento da gravação,
sequencialmente por agregado (match_id). A inserção é idempotente: o mesmo
`event_id` nunca é gravado duas vezes.
"""
from __future__ import annotations

import json
import logging

import asyncpg

from wc2026.domain.events import EventEnvelope, dumps

log = logging.getLogger("wc2026.eventstore")


async def append(pool: asyncpg.Pool, env: EventEnvelope) -> bool:
    """Grava um evento. Retorna True se inseriu, False se já existia (idempotente)."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Próxima versão do agregado (lock implícito via sequência por insert).
            row = await conn.fetchrow(
                "SELECT COALESCE(MAX(version), 0) AS v FROM events WHERE aggregate_id = $1",
                env.aggregate_id,
            )
            next_version = int(row["v"]) + 1
            result = await conn.execute(
                """
                INSERT INTO events
                    (event_id, aggregate_type, aggregate_id, event_type,
                     version, payload, occurred_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT (event_id) DO NOTHING
                """,
                env.event_id, env.aggregate_type, env.aggregate_id,
                env.event_type, next_version, dumps(env.payload), env.occurred_at,
            )
            return result.endswith("1")


async def read_all(pool: asyncpg.Pool, after_seq: int = 0):
    """Itera o log inteiro em ordem (para rebuild de projeções)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT global_seq, event_id, aggregate_id, event_type, version,
                   payload, occurred_at
            FROM events
            WHERE global_seq > $1
            ORDER BY global_seq ASC
            """,
            after_seq,
        )
    for r in rows:
        yield EventEnvelope(
            event_id=r["event_id"],
            aggregate_id=r["aggregate_id"],
            event_type=r["event_type"],
            version=r["version"],
            payload=json.loads(r["payload"]),
            occurred_at=r["occurred_at"],
        )
