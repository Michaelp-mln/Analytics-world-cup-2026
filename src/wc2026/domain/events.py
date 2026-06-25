"""Eventos de domínio (Event Sourcing).

Cada acontecimento de uma partida vira um evento imutável. O `event_id` é
determinístico (uuid5) a partir das características do evento, garantindo
idempotência: reprocessar a mesma jogada da API gera o mesmo id e não duplica.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

# Namespace fixo para gerar event_ids determinísticos.
_NS = uuid.UUID("6f1d4c2a-7c3b-4f4a-9b2e-9f0c1a2b3c4d")


# --- Tipos de evento -------------------------------------------------------
class EventType:
    MATCH_SCHEDULED = "MatchScheduled"
    MATCH_STARTED = "MatchStarted"
    GOAL_SCORED = "GoalScored"
    STATS_SAMPLED = "StatsSampled"      # snapshot cumulativo (posse/finalizações)
    CARD_SHOWN = "CardShown"
    MATCH_FINISHED = "MatchFinished"


def deterministic_id(*parts: Any) -> uuid.UUID:
    """uuid5 estável a partir das partes que identificam unicamente o evento."""
    return uuid.uuid5(_NS, "|".join(str(p) for p in parts))


class EventEnvelope(BaseModel):
    """Envelope padrão que trafega no Kafka e é gravado no event store."""

    event_id: uuid.UUID
    aggregate_type: str = "match"
    aggregate_id: str                      # match_id
    event_type: str
    version: int = 0                       # atribuído no event store
    payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # -- serialização para Kafka --
    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "EventEnvelope":
        return cls.model_validate_json(raw)


# --- Fábricas de eventos ---------------------------------------------------
def match_scheduled(match_id: str, home: str, away: str, *, grp: str | None,
                    stage: str, kickoff: str | None = None) -> EventEnvelope:
    return EventEnvelope(
        event_id=deterministic_id(match_id, EventType.MATCH_SCHEDULED),
        aggregate_id=match_id,
        event_type=EventType.MATCH_SCHEDULED,
        payload={"home": home, "away": away, "grp": grp, "stage": stage,
                 "kickoff": kickoff},
    )


def match_started(match_id: str) -> EventEnvelope:
    return EventEnvelope(
        event_id=deterministic_id(match_id, EventType.MATCH_STARTED),
        aggregate_id=match_id,
        event_type=EventType.MATCH_STARTED,
        payload={},
    )


def goal_scored(match_id: str, team: str, player: str, minute: int,
                *, assist: str | None = None, penalty: bool = False,
                own_goal: bool = False) -> EventEnvelope:
    """`team` = seleção que MARCA o gol (para gol contra, é o adversário do autor)."""
    return EventEnvelope(
        event_id=deterministic_id(match_id, EventType.GOAL_SCORED, team, player, minute),
        aggregate_id=match_id,
        event_type=EventType.GOAL_SCORED,
        payload={"team": team, "player": player, "minute": minute,
                 "assist": assist, "penalty": penalty, "own_goal": own_goal},
    )


def stats_sampled(match_id: str, minute: int, home: str, away: str, *,
                  home_poss: float, away_poss: float,
                  home_shots: int, away_shots: int,
                  home_sot: int, away_sot: int) -> EventEnvelope:
    """Snapshot CUMULATIVO das estatísticas da partida em um dado minuto.

    Posse e finalizações são totais correntes (não incrementos), então o
    projetor faz SET (não soma) — robusto para reprocessamento e idêntico
    para dados da API (que vêm como totais) e do simulador.
    """
    return EventEnvelope(
        event_id=deterministic_id(match_id, EventType.STATS_SAMPLED, minute),
        aggregate_id=match_id,
        event_type=EventType.STATS_SAMPLED,
        payload={"minute": minute, "home": home, "away": away,
                 "home_poss": round(home_poss, 2), "away_poss": round(away_poss, 2),
                 "home_shots": home_shots, "away_shots": away_shots,
                 "home_sot": home_sot, "away_sot": away_sot},
    )


def card_shown(match_id: str, team: str, player: str, minute: int,
               card: str) -> EventEnvelope:
    return EventEnvelope(
        event_id=deterministic_id(match_id, EventType.CARD_SHOWN, team, player, minute, card),
        aggregate_id=match_id,
        event_type=EventType.CARD_SHOWN,
        payload={"team": team, "player": player, "minute": minute, "card": card},
    )


def match_finished(match_id: str, home: str, away: str,
                   home_score: int, away_score: int, *,
                   winner: str | None = None,
                   pen_home: int | None = None,
                   pen_away: int | None = None) -> EventEnvelope:
    """`winner`/`pen_*` só no mata-mata (desempate por pênaltis)."""
    return EventEnvelope(
        event_id=deterministic_id(match_id, EventType.MATCH_FINISHED),
        aggregate_id=match_id,
        event_type=EventType.MATCH_FINISHED,
        payload={"home": home, "away": away,
                 "home_score": home_score, "away_score": away_score,
                 "winner": winner, "pen_home": pen_home, "pen_away": pen_away},
    )


def json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, uuid.UUID)):
        return str(obj)
    raise TypeError(f"não serializável: {type(obj)}")


def dumps(obj: Any) -> str:
    return json.dumps(obj, default=json_default, ensure_ascii=False)
