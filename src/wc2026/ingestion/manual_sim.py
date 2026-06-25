"""Simulador MANUAL — controlado pelo botão do dashboard.

Cada chamada de `next_round` joga a próxima rodada e emite os eventos no Kafka
(mesmo pipeline de event sourcing). Sequência:

    Rodada 1 → Rodada 2 → Rodada 3 (grupos)
    → 32-avos → Oitavas → Quartas → Semis → Disputa de 3º → Final

O mata-mata é montado a partir dos 32 classificados reais (1º, 2º e 8 melhores
3º) com cabeças de chave por rating; empates são decididos nos pênaltis. O
progresso é persistido em `sim_state`, então sobrevive a reinícios da API.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict

import asyncpg

from wc2026.analytics.probability import _seed_slots
from wc2026.domain import events as E
from wc2026.domain.teams import RATING_OF
from wc2026.ingestion.simulator import _Match, _build_fixtures, _code

log = logging.getLogger("wc2026.manual_sim")

KO_ROUNDS = ["R32", "R16", "QF", "SF", "3P", "FINAL"]
LABEL_PT = {
    "R32": "32-avos de final", "R16": "Oitavas de final",
    "QF": "Quartas de final", "SF": "Semifinais",
    "3P": "Disputa de 3º lugar", "FINAL": "Final",
}
_SLOTS32 = _seed_slots(32)


async def _play_match(m: _Match, emit, *, knockout: bool) -> dict:
    """Joga uma partida e emite todos os eventos de uma vez (sem espera)."""
    await emit(E.match_started(m.id))
    hs = as_ = 0
    for minute, side in m.goal_minutes:
        team = m.home if side == "home" else m.away
        scorer, assist = m._scorer(side)
        penalty = m.rng.random() < 0.12
        if side == "home":
            hs += 1
        else:
            as_ += 1
        await emit(E.goal_scored(m.id, team, scorer, minute, assist=assist, penalty=penalty))
    for minute, side in m.card_minutes:
        team = m.home if side == "home" else m.away
        card = "red" if m.rng.random() < 0.12 else "yellow"
        await emit(E.card_shown(m.id, team, f"{_code(team)} #{m.rng.randint(2, 23)}",
                                minute, card))
    hp = max(30.0, min(70.0, m.base_home_poss))
    await emit(E.stats_sampled(
        m.id, 90, m.home, m.away, home_poss=hp, away_poss=100 - hp,
        home_shots=hs * 3 + m.rng.randint(2, 10), away_shots=as_ * 3 + m.rng.randint(2, 10),
        home_sot=hs + m.rng.randint(1, 4), away_sot=as_ + m.rng.randint(1, 4)))

    winner = pen_h = pen_a = None
    if knockout:
        if hs == as_:  # empate -> pênaltis
            ph, pa = RATING_OF.get(m.home, 72), RATING_OF.get(m.away, 72)
            home_adv = m.rng.random() < ph / (ph + pa)
            winner = m.home if home_adv else m.away
            pen_h, pen_a = (5, 4) if home_adv else (4, 5)
        else:
            winner = m.home if hs > as_ else m.away
    await emit(E.match_finished(m.id, m.home, m.away, hs, as_,
                                winner=winner, pen_home=pen_h, pen_away=pen_a))
    loser = (m.away if winner == m.home else m.home) if knockout else None
    return {"match_id": m.id, "home": m.home, "away": m.away,
            "home_score": hs, "away_score": as_, "winner": winner, "loser": loser,
            "pen_home": pen_h, "pen_away": pen_a}


class ManualTournament:
    def __init__(self) -> None:
        self.group_rounds = _build_fixtures()        # {1,2,3 -> [_Match]}
        self.group_played = 0
        self.ko_index = 0
        self.qualifiers: list[str] = []              # 32 nomes em ordem de chave
        self.ko_winners: dict[str, list[str]] = {}
        self.ko_losers: dict[str, list[str]] = {}
        self._lock = asyncio.Lock()
        self._scheduled = False

    # ----- persistência -----
    async def load(self, pool: asyncpg.Pool) -> None:
        row = await pool.fetchrow(
            "SELECT group_played, ko_index, state FROM sim_state WHERE id = 1")
        if not row:
            return
        self.group_played = row["group_played"]
        self.ko_index = row["ko_index"]
        st = row["state"]
        st = st if isinstance(st, dict) else json.loads(st or "{}")
        self.qualifiers = st.get("qualifiers", [])
        self.ko_winners = st.get("ko_winners", {})
        self.ko_losers = st.get("ko_losers", {})
        log.info("Estado do torneio restaurado: grupos=%d ko=%d",
                 self.group_played, self.ko_index)

    async def _save(self, pool: asyncpg.Pool) -> None:
        st = json.dumps({"qualifiers": self.qualifiers,
                         "ko_winners": self.ko_winners, "ko_losers": self.ko_losers})
        await pool.execute(
            """
            INSERT INTO sim_state (id, group_played, ko_index, state)
            VALUES (1, $1, $2, $3::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                group_played = EXCLUDED.group_played,
                ko_index = EXCLUDED.ko_index, state = EXCLUDED.state
            """, self.group_played, self.ko_index, st)

    async def ensure_scheduled(self, emit, pool: asyncpg.Pool) -> None:
        if self._scheduled:
            return
        for rnd in (1, 2, 3):
            for m in self.group_rounds[rnd]:
                await emit(E.match_scheduled(m.id, m.home, m.away,
                                             grp=m.grp, stage="group", kickoff=None))
        self._scheduled = True
        log.info("Fixtures de grupo agendados (modo manual).")

    # ----- estado para a UI -----
    def state(self) -> dict:
        if self.group_played < 3:
            phase, nxt = "group", f"Rodada {self.group_played + 1} (fase de grupos)"
        elif self.ko_index < len(KO_ROUNDS):
            phase, nxt = "knockout", LABEL_PT[KO_ROUNDS[self.ko_index]]
        else:
            phase, nxt = "done", "Torneio encerrado 🏆"
        return {
            "phase": phase, "next_label": nxt,
            "done": phase == "done",
            "group_played": self.group_played, "ko_index": self.ko_index,
            "rounds_played": self.group_played + self.ko_index,
            "rounds_total": 3 + len(KO_ROUNDS),
            "champion": (self.ko_winners.get("FINAL") or [None])[0],
        }

    # ----- chaveamento -----
    async def _compute_qualifiers(self, pool: asyncpg.Pool) -> list[str]:
        rows = await pool.fetch("SELECT team, grp, points, gf, ga FROM proj_standings")
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            groups[r["grp"]].append({"team": r["team"], "pts": r["points"],
                                     "gd": r["gf"] - r["ga"], "gf": r["gf"]})
        quals: list[str] = []
        thirds: list[dict] = []
        for mem in groups.values():
            mem.sort(key=lambda x: (x["pts"], x["gd"], x["gf"]), reverse=True)
            if len(mem) >= 2:
                quals += [mem[0]["team"], mem[1]["team"]]
            if len(mem) >= 3:
                thirds.append(mem[2])
        thirds.sort(key=lambda x: (x["pts"], x["gd"], x["gf"]), reverse=True)
        quals += [t["team"] for t in thirds[:8]]
        if len(quals) != 32:
            return []
        seeded = sorted(quals, key=lambda t: RATING_OF.get(t, 72), reverse=True)
        return [seeded[s] for s in _SLOTS32]

    def _ko_pairs(self, label: str) -> list[tuple[str, str]]:
        if label == "R32":
            teams = self.qualifiers
        elif label == "R16":
            teams = self.ko_winners["R32"]
        elif label == "QF":
            teams = self.ko_winners["R16"]
        elif label == "SF":
            teams = self.ko_winners["QF"]
        elif label == "3P":
            return [tuple(self.ko_losers["SF"])]
        elif label == "FINAL":
            return [tuple(self.ko_winners["SF"])]
        else:
            return []
        return [(teams[2 * i], teams[2 * i + 1]) for i in range(len(teams) // 2)]

    # ----- ação principal -----
    async def next_round(self, emit, pool: asyncpg.Pool) -> dict:
        async with self._lock:
            await self.ensure_scheduled(emit, pool)

            if self.group_played < 3:
                rnd = self.group_played + 1
                results = [await _play_match(m, emit, knockout=False)
                           for m in self.group_rounds[rnd]]
                self.group_played = rnd
                await self._save(pool)
                return {"played": {"phase": "group", "label": f"Rodada {rnd}",
                                   "matches": results}, "state": self.state()}

            if self.ko_index < len(KO_ROUNDS):
                label = KO_ROUNDS[self.ko_index]
                if label == "R32" and not self.qualifiers:
                    self.qualifiers = await self._compute_qualifiers(pool)
                    if not self.qualifiers:
                        return {"error": "Classificação ainda consolidando — "
                                         "aguarde um instante e clique de novo.",
                                "state": self.state()}
                matches = []
                for i, (home, away) in enumerate(self._ko_pairs(label)):
                    m = _Match(f"SIM-KO-{label}-{i:02d}", None, home, away)
                    await emit(E.match_scheduled(m.id, home, away, grp=None,
                                                 stage="knockout", kickoff=None))
                    matches.append(m)
                results, winners, losers = [], [], []
                for m in matches:
                    r = await _play_match(m, emit, knockout=True)
                    results.append(r); winners.append(r["winner"]); losers.append(r["loser"])
                self.ko_winners[label] = winners
                self.ko_losers[label] = losers
                self.ko_index += 1
                await self._save(pool)
                return {"played": {"phase": "knockout", "label": LABEL_PT[label],
                                   "matches": results}, "state": self.state()}

            return {"played": None, "state": self.state()}

    async def reset(self, emit, pool: asyncpg.Pool) -> dict:
        async with self._lock:
            async with pool.acquire() as conn:
                await conn.execute(
                    "TRUNCATE events, proj_processed, proj_match, proj_scorers, "
                    "proj_match_stats, proj_discipline, proj_standings, "
                    "proj_goal_timing, sim_state")
            self.group_rounds = _build_fixtures()
            self.group_played = 0
            self.ko_index = 0
            self.qualifiers = []
            self.ko_winners = {}
            self.ko_losers = {}
            self._scheduled = False
            await self.ensure_scheduled(emit, pool)
            log.info("Torneio reiniciado.")
            return self.state()
