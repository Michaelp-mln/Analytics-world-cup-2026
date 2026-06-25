"""Simulador de partidas — fonte autônoma de eventos quando não há chave da API.

Monta um torneio sintético (12 grupos × 3 rodadas), joga as partidas de cada
rodada concorrentemente em tempo comprimido e emite eventos de domínio idênticos
em formato aos que viriam da API-Football. Determinístico por match_id.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from typing import Awaitable, Callable

from wc2026.config import settings
from wc2026.domain import events as E
from wc2026.domain.events import EventEnvelope
from wc2026.domain.teams import GROUPS, RATING_OF, teams_in_group

log = logging.getLogger("wc2026.simulator")

Emit = Callable[[EventEnvelope], Awaitable[None]]

# Confrontos round-robin de um grupo de 4 (índices), por rodada.
_ROUND_ROBIN = {
    1: [(0, 1), (2, 3)],
    2: [(0, 2), (3, 1)],
    3: [(0, 3), (1, 2)],
}


def _code(team: str) -> str:
    from wc2026.domain.teams import BY_NAME
    return BY_NAME.get(team, {}).get("code", team[:3].upper())


def _expected_goals(att: int, dfn: int, base: float) -> float:
    factor = 1.0 + (att - dfn) / 40.0
    return max(0.2, min(4.5, base * factor))


def _poisson(lam: float, rng: random.Random) -> int:
    # Knuth
    import math
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= L:
            return k - 1


class _Match:
    """Estado de uma partida sintética."""

    def __init__(self, match_id: str, grp: str, home: str, away: str):
        self.id = match_id
        self.grp = grp
        self.home, self.away = home, away
        # Seed ESTÁVEL (independe do PYTHONHASHSEED): o mesmo jogo produz sempre
        # o mesmo resultado -> mesmos event_ids -> idempotente entre reinícios.
        seed = int(hashlib.md5(match_id.encode()).hexdigest()[:8], 16)
        self.rng = random.Random(seed)
        self.home_score = self.away_score = 0
        self.home_shots = self.away_shots = 0
        self.home_sot = self.away_sot = 0
        # posse base a partir das forças
        hr, ar = RATING_OF.get(home, 72), RATING_OF.get(away, 72)
        self.base_home_poss = 50 + (hr - ar) * 0.6
        # gols planejados (minuto, lado)
        hg = _poisson(_expected_goals(hr, ar, 1.5), self.rng)
        ag = _poisson(_expected_goals(ar, hr, 1.2), self.rng)
        self.goal_minutes = sorted(
            [(self.rng.randint(2, 90), "home") for _ in range(hg)]
            + [(self.rng.randint(2, 90), "away") for _ in range(ag)]
        )
        self.card_minutes = sorted(
            (self.rng.randint(5, 90), self.rng.choice(["home", "away"]))
            for _ in range(self.rng.randint(0, 5))
        )

    def _scorer(self, side: str) -> tuple[str, str | None]:
        team = self.home if side == "home" else self.away
        num = self.rng.choices([9, 10, 11, 7, 19, 8, 17, 22, 4],
                               weights=[6, 6, 5, 4, 3, 2, 2, 1, 1])[0]
        assist_num = self.rng.choice([8, 10, 6, 11, 20])
        scorer = f"{_code(team)} #{num}"
        assist = f"{_code(team)} #{assist_num}" if self.rng.random() < 0.6 else None
        return scorer, assist


async def _play_match(m: _Match, emit: Emit) -> None:
    spm = settings.sim_seconds_per_minute
    await emit(E.match_started(m.id))
    goals = {gm[0]: gm for gm in m.goal_minutes}
    cards = {cm[0]: cm for cm in m.card_minutes}

    for minute in range(1, 91):
        # gol planejado neste minuto?
        if minute in goals:
            _, side = goals[minute]
            team = m.home if side == "home" else m.away
            scorer, assist = m._scorer(side)
            penalty = m.rng.random() < 0.12
            if side == "home":
                m.home_score += 1
            else:
                m.away_score += 1
            await emit(E.goal_scored(m.id, team, scorer, minute,
                                     assist=assist, penalty=penalty))
        # cartão?
        if minute in cards:
            _, side = cards[minute]
            team = m.home if side == "home" else m.away
            card = "red" if m.rng.random() < 0.12 else "yellow"
            await emit(E.card_shown(m.id, team, f"{_code(team)} #{m.rng.randint(2, 23)}",
                                    minute, card))
        # finalizações acumuladas
        if m.rng.random() < 0.25:
            if m.rng.random() < 0.5:
                m.home_shots += 1
                m.home_sot += 1 if m.rng.random() < 0.4 else 0
            else:
                m.away_shots += 1
                m.away_sot += 1 if m.rng.random() < 0.4 else 0
        # snapshot de estatísticas a cada ~10 min
        if minute % 10 == 0 or minute in goals:
            jitter = m.rng.uniform(-6, 6)
            hp = max(20.0, min(80.0, m.base_home_poss + jitter))
            await emit(E.stats_sampled(
                m.id, minute, m.home, m.away,
                home_poss=hp, away_poss=100 - hp,
                home_shots=m.home_shots, away_shots=m.away_shots,
                home_sot=m.home_sot, away_sot=m.away_sot))
        await asyncio.sleep(spm)

    await emit(E.match_finished(m.id, m.home, m.away, m.home_score, m.away_score))
    log.info("FT %s %d x %d %s", m.home, m.home_score, m.away_score, m.away)


def _build_fixtures() -> dict[int, list[_Match]]:
    by_round: dict[int, list[_Match]] = {1: [], 2: [], 3: []}
    for grp in GROUPS:
        teams = teams_in_group(grp)
        if len(teams) < 4:
            continue
        for rnd, pairs in _ROUND_ROBIN.items():
            for gi, (i, j) in enumerate(pairs):
                mid = f"SIM-{grp}-R{rnd}-G{gi}"
                by_round[rnd].append(_Match(mid, grp, teams[i], teams[j]))
    return by_round


async def run(emit: Emit) -> None:
    """Loop principal do simulador."""
    fixtures = _build_fixtures()

    # 1) agenda todas as partidas do torneio
    for rnd in (1, 2, 3):
        for m in fixtures[rnd]:
            await emit(E.match_scheduled(m.id, m.home, m.away,
                                         grp=m.grp, stage="group", kickoff=None))
    log.info("Torneio simulado agendado: %d partidas",
             sum(len(v) for v in fixtures.values()))

    # 2) joga rodada a rodada (jogos da rodada em paralelo)
    for rnd in (1, 2, 3):
        log.info("=== Rodada %d: %d jogos ao vivo ===", rnd, len(fixtures[rnd]))
        await asyncio.gather(*(_play_match(m, emit) for m in fixtures[rnd]))
        await asyncio.sleep(15)   # intervalo entre rodadas

    log.info("Fase de grupos simulada concluída. Ingestão em repouso.")
    while True:
        await asyncio.sleep(3600)
