"""Probabilidade de resultado AO VIVO (vitória/empate/derrota).

Para cada jogo em andamento, modela os gols do tempo restante como Poisson
(λ proporcional ao tempo que falta e à força ofensiva/defensiva das seleções)
e soma analiticamente a distribuição de placares finais a partir do placar atual.
"""
from __future__ import annotations

import math

import asyncpg

from wc2026.analytics.probability import _HOME_ADV, _LEAGUE_AVG, _strength
from wc2026.domain.teams import RATING_OF

_MAXG = 8   # gols adicionais considerados por lado (cauda desprezível além disso)


def _pmf(k: int, lam: float) -> float:
    return math.exp(-lam) * lam ** k / math.factorial(k)


def _result_probs(hs: int, as_: int, lam_h: float, lam_a: float) -> tuple[float, float, float]:
    home = draw = away = 0.0
    dist_h = [_pmf(k, lam_h) for k in range(_MAXG + 1)]
    dist_a = [_pmf(k, lam_a) for k in range(_MAXG + 1)]
    for i, pi in enumerate(dist_h):
        for j, pj in enumerate(dist_a):
            p = pi * pj
            fh, fa = hs + i, as_ + j
            if fh > fa:
                home += p
            elif fh == fa:
                draw += p
            else:
                away += p
    tot = home + draw + away or 1.0
    return round(100 * home / tot, 1), round(100 * draw / tot, 1), round(100 * away / tot, 1)


async def compute_live(pool: asyncpg.Pool) -> list[dict]:
    live = await pool.fetch(
        "SELECT match_id, grp, home, away, home_score, away_score, minute "
        "FROM proj_match WHERE status = 'live' ORDER BY minute DESC")
    if not live:
        return []

    rows = await pool.fetch("SELECT team, played, gf, ga FROM proj_standings")
    strength = {r["team"]: _strength(r["played"], r["gf"], r["ga"],
                                     RATING_OF.get(r["team"], 75)) for r in rows}
    neutral = (_LEAGUE_AVG, _LEAGUE_AVG)

    out = []
    for m in live:
        a_h, d_h = strength.get(m["home"], neutral)
        a_a, d_a = strength.get(m["away"], neutral)
        rem = max(0, 90 - int(m["minute"] or 0)) / 90.0
        lam_h = max(0.02, _HOME_ADV * a_h * d_a / _LEAGUE_AVG) * rem
        lam_a = max(0.02, a_a * d_h / _LEAGUE_AVG) * rem
        ph, pd, pa = _result_probs(int(m["home_score"]), int(m["away_score"]), lam_h, lam_a)
        out.append({
            "match_id": m["match_id"], "home": m["home"], "away": m["away"],
            "home_score": m["home_score"], "away_score": m["away_score"],
            "minute": m["minute"], "p_home": ph, "p_draw": pd, "p_away": pa,
        })
    return out
