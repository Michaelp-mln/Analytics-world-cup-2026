"""Helpers de Kafka (aiokafka) com retry de conexão no startup."""
from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from wc2026.config import settings

log = logging.getLogger("wc2026.kafka")


async def make_producer(retries: int = 30, delay: float = 2.0) -> AIOKafkaProducer:
    """Cria e inicia um produtor, aguardando o broker ficar disponível."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            linger_ms=20,
        )
        try:
            await producer.start()
            log.info("Produtor Kafka conectado em %s", settings.kafka_bootstrap_servers)
            return producer
        except KafkaConnectionError as exc:
            last_err = exc
            await producer.stop()
            log.warning("Kafka indisponível (tentativa %d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(delay)
    raise RuntimeError(f"Não foi possível conectar ao Kafka: {last_err}")


async def make_consumer(topic: str, group_id: str, *, retries: int = 30,
                        delay: float = 2.0, auto_offset_reset: str = "earliest"
                        ) -> AIOKafkaConsumer:
    """Cria e inicia um consumidor de um grupo, aguardando o broker."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=2000,
        )
        try:
            await consumer.start()
            log.info("Consumidor '%s' conectado (topic=%s)", group_id, topic)
            return consumer
        except KafkaConnectionError as exc:
            last_err = exc
            await consumer.stop()
            log.warning("Kafka indisponível (tentativa %d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(delay)
    raise RuntimeError(f"Não foi possível conectar ao Kafka: {last_err}")
