"""Corrida pela Chuteira de Ouro (artilheiro do torneio).

Combina os gols já marcados com uma projeção do restante:
  λ_jogador = (gols / jogos do time) × (jogos que ainda faltam ao time)
onde "jogos que faltam" = jogos de grupo restantes + nº esperado de partidas
de mata-mata (derivado das probabilidades de alcançar cada fase). Um Monte
Carlo amostra os gols adicionais (Poisson) e conta com que frequência cada
candidato termina como maior goleador → probabilidade de Chuteira de Ouro.
"""
from __future__ import annotations

from collections import defaultdict

import asyncpg
import numpy as np

_RUNS = 5000
_TOP = 30        # candidatos considerados (fora do top-30 a chance é desprezível)


def _simulate_race(base: np.ndarray, lam: np.ndarray, runs: int = _RUNS,
                   seed: int = 2026) -> tuple[np.ndarray, np.ndarray]:
    """Retorna (prob_chuteira, gols_finais_esperados) por candidato."""
    rng = np.random.default_rng(seed)
    n = len(base)
    wins = np.zeros(n)
    proj = np.zeros(n)
    for _ in range(runs):
        final = base + rng.poisson(lam)
        proj += final
        mx = final.max()
        w = np.flatnonzero(final == mx)
        wins[w] += 1.0 / len(w)        # empate divide o crédito
    return wins / runs, proj / runs


async def compute(pool: asyncpg.Pool, title: list[dict]) -> dict:
    scorers = await pool.fetch(
        """
        SELECT player, team, goals, penalties, assists
        FROM proj_scorers WHERE goals > 0
        ORDER BY goals DESC, assists DESC, player ASC LIMIT $1
        """, _TOP)
    if not scorers:
        return {"runs": 0, "contenders": []}

    played = {r["team"]: r["played"]
              for r in await pool.fetch("SELECT team, played FROM proj_standings")}

    rem_group: dict[str, int] = defaultdict(int)
    for r in await pool.fetch(
            "SELECT home, away FROM proj_match "
            "WHERE status <> 'finished' AND grp IS NOT NULL"):
        rem_group[r["home"]] += 1
        rem_group[r["away"]] += 1

    # nº esperado de jogos de mata-mata = soma das prob. de jogar cada fase
    reach = {t["team"]: (t["p_advance"] + t["p_r16"] + t["p_qf"]
                         + t["p_sf"] + t["p_final"]) / 100.0 for t in title}

    base, lam, meta = [], [], []
    for s in scorers:
        apps = max(played.get(s["team"], 0), 1)
        rate = s["goals"] / apps
        exp_matches = rem_group.get(s["team"], 0) + reach.get(s["team"], 0.0)
        base.append(s["goals"])
        lam.append(max(0.0, rate * exp_matches))
        meta.append(s)

    wins, proj = _simulate_race(np.array(base, float), np.array(lam, float))

    contenders = []
    for i, s in enumerate(meta):
        contenders.append({
            "player": s["player"], "team": s["team"],
            "goals": s["goals"], "penalties": s["penalties"], "assists": s["assists"],
            "exp_remaining_goals": round(float(lam[i]), 2),
            "proj_final_goals": round(float(proj[i]), 1),
            "p_golden_boot": round(100 * float(wins[i]), 1),
        })
    contenders.sort(key=lambda r: (r["goals"], r["p_golden_boot"]), reverse=True)
    return {"runs": _RUNS, "contenders": contenders}
