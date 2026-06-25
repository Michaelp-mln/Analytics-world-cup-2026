"""Consumidor que materializa o EVENT STORE.

Lê o tópico `match.events` e grava cada evento na tabela append-only `events`
(fonte da verdade do event sourcing). A gravação é idempotente por `event_id`.
"""
from __future__ import annotations

import asyncio
import logging

from wc2026.config import settings
from wc2026.domain.events import EventEnvelope
from wc2026.infra import eventstore
from wc2026.infra.db import make_pool
from wc2026.infra.kafka import make_consumer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("wc2026.eventstore_writer")


async def main() -> None:
    pool = await make_pool()
    consumer = await make_consumer(settings.kafka_topic_events,
                                   group_id="eventstore-writer")
    log.info("Gravador do event store iniciado.")
    try:
        async for msg in consumer:
            env = EventEnvelope.from_bytes(msg.value)
            inserted = await eventstore.append(pool, env)
            if inserted:
                log.info("append %s v? %s/%s", env.event_type,
                         env.aggregate_id, env.event_id)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
