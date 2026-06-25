"""Serviço de ingestão (produtor Kafka).

Escolhe a fonte (API-Football ou simulador) e publica eventos de domínio no
tópico `match.events`, particionando por match_id para preservar a ordem por
partida. A deduplicação acontece naturalmente a jusante via `event_id`
determinístico (idempotência).
"""
from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer

from wc2026.config import settings
from wc2026.domain.events import EventEnvelope
from wc2026.infra.kafka import make_producer
from wc2026.ingestion import simulator, transform
from wc2026.ingestion.api_football import APIFootballClient

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("wc2026.ingestion")


def make_emitter(producer: AIOKafkaProducer):
    topic = settings.kafka_topic_events

    async def emit(env: EventEnvelope) -> None:
        await producer.send_and_wait(
            topic, value=env.to_bytes(), key=env.aggregate_id.encode("utf-8"))

    return emit


async def run_api(emit) -> None:
    """Loop de polling da API-Football com sincronização periódica completa."""
    client = APIFootballClient()
    group_map: dict[int, str] = {}
    poll = 0
    try:
        while True:
            # Sincronização completa (agenda + encerrados + grupos) periodicamente.
            if poll % 10 == 0:
                try:
                    group_map = await client.team_group_map() or group_map
                    for fx in await client.all_fixtures():
                        for ev in transform.fixture_lifecycle(fx, group_map):
                            await emit(ev)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Falha na sincronização completa: %s", exc)

            # Jogos ao vivo: ciclo de vida + eventos + estatísticas.
            try:
                live = await client.live_fixtures()
                log.info("%d jogo(s) ao vivo", len(live))
                for fx in live:
                    fid = fx.get("fixture", {}).get("id")
                    for ev in transform.fixture_lifecycle(fx, group_map):
                        await emit(ev)
                    for ev in transform.parse_match_events(fx, await client.fixture_events(fid)):
                        await emit(ev)
                    stat = transform.parse_statistics(fx, await client.fixture_statistics(fid))
                    if stat is not None:
                        await emit(stat)
            except Exception as exc:  # noqa: BLE001
                log.warning("Falha ao processar jogos ao vivo: %s", exc)

            poll += 1
            await asyncio.sleep(settings.poll_interval_seconds)
    finally:
        await client.close()


async def main() -> None:
    source = settings.effective_source

    # No modo manual o simulador é dirigido pela API (botão do dashboard),
    # então o serviço de ingestão apenas permanece ocioso.
    if source == "simulator" and settings.sim_mode == "manual":
        log.info("Modo MANUAL: ingestão ociosa; a simulação é controlada "
                 "pelo dashboard (POST /api/sim/next-round).")
        while True:
            await asyncio.sleep(3600)

    if settings.ingestion_source == "api" and source == "simulator":
        log.warning("INGESTION_SOURCE=api mas API_FOOTBALL_KEY vazio "
                    "-> caindo para o SIMULADOR.")
    log.info("Fonte de ingestão efetiva: %s", source)

    producer = await make_producer()
    emit = make_emitter(producer)
    try:
        if source == "api":
            await run_api(emit)
        else:
            await simulator.run(emit)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
