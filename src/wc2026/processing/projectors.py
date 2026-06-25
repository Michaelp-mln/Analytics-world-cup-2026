"""Projetores assíncronos: eventos -> READ MODELS (projeções).

Consome `match.events` e atualiza as tabelas `proj_*`. Cada evento é aplicado
no máximo uma vez (guarda de idempotência `proj_processed`), tornando o
processamento seguro sob entrega "at-least-once" do Kafka.

Uso:
    python -m wc2026.processing.projectors            # consumidor contínuo
    python -m wc2026.processing.projectors --rebuild  # reconstrói do event store
"""
from __future__ import annotations

import asyncio
import logging
import sys

import asyncpg

from wc2026.config import settings
from wc2026.domain.events import EventType, EventEnvelope
from wc2026.infra import eventstore
from wc2026.infra.db import make_pool
from wc2026.infra.kafka import make_consumer
from wc2026.infra.migrate import ensure_schema

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("wc2026.projector")


# --- aplicação de cada evento --------------------------------------------
async def _apply_scheduled(conn, mid: str, p: dict) -> None:
    await conn.execute(
        """
        INSERT INTO proj_match (match_id, grp, stage, home, away, kickoff, status)
        VALUES ($1,$2,$3,$4,$5,$6,'scheduled')
        ON CONFLICT (match_id) DO UPDATE SET
            grp=EXCLUDED.grp, stage=EXCLUDED.stage,
            home=EXCLUDED.home, away=EXCLUDED.away, kickoff=EXCLUDED.kickoff
        """,
        mid, p.get("grp"), p.get("stage"), p["home"], p["away"],
        _parse_ts(p.get("kickoff")))
    if p.get("grp"):
        for team in (p["home"], p["away"]):
            await conn.execute(
                "INSERT INTO proj_standings (team, grp) VALUES ($1,$2) "
                "ON CONFLICT (team) DO NOTHING", team, p["grp"])


async def _apply_started(conn, mid: str) -> None:
    await conn.execute(
        "UPDATE proj_match SET status='live', updated_at=now() "
        "WHERE match_id=$1 AND status='scheduled'", mid)


def _time_bucket(minute: int) -> str | None:
    if minute <= 0:
        return None
    if minute > 90:
        return "90+"
    hi = ((minute - 1) // 15 + 1) * 15
    return f"{hi - 14}-{hi}"


async def _apply_goal(conn, mid: str, p: dict) -> None:
    team, player = p["team"], p["player"]
    minute = int(p.get("minute") or 0)
    await conn.execute(
        """
        UPDATE proj_match SET
            home_score = home_score + (home = $2)::int,
            away_score = away_score + (away = $2)::int,
            minute = GREATEST(minute, $3), updated_at = now()
        WHERE match_id = $1
        """, mid, team, minute)
    bucket = _time_bucket(minute)
    if bucket:
        await conn.execute(
            "INSERT INTO proj_goal_timing (bucket, goals) VALUES ($1, 1) "
            "ON CONFLICT (bucket) DO UPDATE SET goals = proj_goal_timing.goals + 1",
            bucket)
    if p.get("own_goal"):
        return  # gol contra não credita o jogador como artilheiro
    await conn.execute(
        """
        INSERT INTO proj_scorers (player, team, goals, penalties)
        VALUES ($1,$2,1,$3)
        ON CONFLICT (player, team) DO UPDATE SET
            goals = proj_scorers.goals + 1,
            penalties = proj_scorers.penalties + $3
        """, player, team, 1 if p.get("penalty") else 0)
    if p.get("assist"):
        await conn.execute(
            """
            INSERT INTO proj_scorers (player, team, assists) VALUES ($1,$2,1)
            ON CONFLICT (player, team) DO UPDATE SET
                assists = proj_scorers.assists + 1
            """, p["assist"], team)


async def _apply_stats(conn, mid: str, p: dict) -> None:
    for side in ("home", "away"):
        team = p[side]
        await conn.execute(
            """
            INSERT INTO proj_match_stats
                (match_id, team, grp, possession, shots, shots_on_target)
            VALUES ($1,$2,(SELECT grp FROM proj_match WHERE match_id=$1),$3,$4,$5)
            ON CONFLICT (match_id, team) DO UPDATE SET
                possession=EXCLUDED.possession, shots=EXCLUDED.shots,
                shots_on_target=EXCLUDED.shots_on_target, updated_at=now()
            """, mid, team, p[f"{side}_poss"], p[f"{side}_shots"], p[f"{side}_sot"])
    await conn.execute(
        """
        UPDATE proj_match SET home_possession=$2, away_possession=$3,
            minute=GREATEST(minute,$4), updated_at=now()
        WHERE match_id=$1
        """, mid, p["home_poss"], p["away_poss"], int(p.get("minute") or 0))


async def _apply_card(conn, mid: str, p: dict) -> None:
    y, r = (1, 0) if p["card"] == "yellow" else (0, 1)
    await conn.execute(
        """
        INSERT INTO proj_discipline (team, grp, yellow, red)
        VALUES ($1,(SELECT grp FROM proj_match WHERE match_id=$2),$3,$4)
        ON CONFLICT (team) DO UPDATE SET
            yellow = proj_discipline.yellow + $3,
            red    = proj_discipline.red + $4
        """, p["team"], mid, y, r)


async def _apply_finished(conn, mid: str, p: dict) -> None:
    hs, as_ = int(p["home_score"]), int(p["away_score"])
    await conn.execute(
        """
        UPDATE proj_match SET status='finished', home_score=$2, away_score=$3,
            minute=GREATEST(minute,90), winner=$4, pen_home=$5, pen_away=$6,
            updated_at=now()
        WHERE match_id=$1
        """, mid, hs, as_, p.get("winner"), p.get("pen_home"), p.get("pen_away"))

    grp = await conn.fetchval("SELECT grp FROM proj_match WHERE match_id=$1", mid)
    if not grp:
        return  # mata-mata não entra na classificação de grupos

    def line(gf, ga):
        if gf > ga:
            return (1, 0, 0, 3)
        if gf == ga:
            return (0, 1, 0, 1)
        return (0, 0, 1, 0)

    for team, gf, ga in ((p["home"], hs, as_), (p["away"], as_, hs)):
        w, d, l, pts = line(gf, ga)
        cs = 1 if ga == 0 else 0           # clean sheet = não sofreu gol
        await conn.execute(
            "INSERT INTO proj_standings (team, grp) VALUES ($1,$2) "
            "ON CONFLICT (team) DO NOTHING", team, grp)
        await conn.execute(
            """
            UPDATE proj_standings SET
                played=played+1, won=won+$2, drawn=drawn+$3, lost=lost+$4,
                gf=gf+$5, ga=ga+$6, points=points+$7, clean_sheets=clean_sheets+$8
            WHERE team=$1
            """, team, w, d, l, gf, ga, pts, cs)


_DISPATCH = {
    EventType.MATCH_SCHEDULED: lambda c, m, p: _apply_scheduled(c, m, p),
    EventType.MATCH_STARTED:   lambda c, m, p: _apply_started(c, m),
    EventType.GOAL_SCORED:     lambda c, m, p: _apply_goal(c, m, p),
    EventType.STATS_SAMPLED:   lambda c, m, p: _apply_stats(c, m, p),
    EventType.CARD_SHOWN:      lambda c, m, p: _apply_card(c, m, p),
    EventType.MATCH_FINISHED:  lambda c, m, p: _apply_finished(c, m, p),
}


def _parse_ts(value):
    if not value:
        return None
    from datetime import datetime
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


async def apply(conn: asyncpg.Connection, env: EventEnvelope) -> bool:
    """Aplica um evento às projeções, uma única vez. Retorna True se aplicou."""
    res = await conn.execute(
        "INSERT INTO proj_processed (event_id) VALUES ($1) ON CONFLICT DO NOTHING",
        env.event_id)
    if not res.endswith("1"):
        return False  # já processado
    handler = _DISPATCH.get(env.event_type)
    if handler:
        await handler(conn, env.aggregate_id, env.payload)
    return True


# --- modos de execução ----------------------------------------------------
async def consume() -> None:
    pool = await make_pool()
    await ensure_schema(pool)
    consumer = await make_consumer(settings.kafka_topic_events, group_id="projector")
    log.info("Projetor iniciado (consumindo %s).", settings.kafka_topic_events)
    try:
        async for msg in consumer:
            env = EventEnvelope.from_bytes(msg.value)
            async with pool.acquire() as conn:
                async with conn.transaction():
                    applied = await apply(conn, env)
            if applied:
                log.info("proj <- %s (%s)", env.event_type, env.aggregate_id)
    finally:
        await consumer.stop()
        await pool.close()


async def rebuild() -> None:
    """Reconstrói TODAS as projeções relendo o event store em ordem."""
    pool = await make_pool()
    await ensure_schema(pool)
    log.info("Rebuild: limpando projeções...")
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE proj_processed, proj_match, proj_scorers, "
            "proj_match_stats, proj_discipline, proj_standings, proj_goal_timing")
    count = 0
    async for env in eventstore.read_all(pool):
        async with pool.acquire() as conn:
            async with conn.transaction():
                await apply(conn, env)
        count += 1
    log.info("Rebuild concluído: %d eventos reaplicados.", count)
    await pool.close()


def main() -> None:
    if "--rebuild" in sys.argv:
        asyncio.run(rebuild())
    else:
        asyncio.run(consume())


if __name__ == "__main__":
    main()
