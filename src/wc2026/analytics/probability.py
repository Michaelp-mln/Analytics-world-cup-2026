"""Probabilidade de classificação E de título via Monte Carlo.

Formato 2026: 12 grupos de 4. Avançam 1º, 2º e os 8 melhores 3º (32 times).
A simulação:
  1) sorteia o restante da fase de grupos (Poisson) e ordena cada grupo;
  2) monta os 32 classificados e simula TODO o mata-mata (32→16→8→4→2→campeão)
     num chaveamento por força (cabeças de chave), decidindo empates nos pênaltis;
  3) acumula, por seleção, a frequência de avançar do grupo e de alcançar cada
     fase — incluindo o título.
"""
from __future__ import annotations

import asyncio
import logging

import asyncpg
import numpy as np

from wc2026.config import settings
from wc2026.domain.teams import RATING_OF

log = logging.getLogger("wc2026.probability")

_LEAGUE_AVG = 1.30
_HOME_ADV = 1.08
_PRIOR_MATCHES = 2.0


def _strength(played: int, gf: int, ga: int, rating: float) -> tuple[float, float]:
    """(ataque, defesa) em gols/jogo: observado suavizado por prior de rating."""
    prior_att = _LEAGUE_AVG * (rating / 75.0)
    prior_def = _LEAGUE_AVG * (75.0 / rating)
    att = (gf + prior_att * _PRIOR_MATCHES) / (played + _PRIOR_MATCHES)
    dfn = (ga + prior_def * _PRIOR_MATCHES) / (played + _PRIOR_MATCHES)
    return att, dfn


def _seed_slots(n: int) -> list[int]:
    """Ordem de chaveamento por cabeça de chave (1 e 2 só se encontram na final)."""
    order = [0]
    while len(order) < n:
        m = len(order) * 2
        nxt = []
        for x in order:
            nxt.append(x)
            nxt.append(m - 1 - x)
        order = nxt
    return order


_SLOTS_32 = _seed_slots(32)


def _simulate(teams, group_of, base_pts, base_gf, base_ga,
              fx_home, fx_away, lam_home, lam_away,
              att, dfn, power, runs: int) -> dict:
    rng = np.random.default_rng(20260623)
    n = len(teams)
    idx = {t: i for i, t in enumerate(teams)}
    groups: dict[str, list[int]] = {}
    for t, g in group_of.items():
        groups.setdefault(g, []).append(idx[t])

    adv = np.zeros(n); top2 = np.zeros(n); win = np.zeros(n); pts_sum = np.zeros(n)
    r16 = np.zeros(n); qf = np.zeros(n); sf = np.zeros(n)
    fin = np.zeros(n); champ = np.zeros(n)
    bracket_runs = 0
    has_fx = len(fx_home) > 0

    def play(a: int, b: int) -> int:
        la = min(5.0, max(0.15, att[a] * dfn[b] / _LEAGUE_AVG))
        lb = min(5.0, max(0.15, att[b] * dfn[a] / _LEAGUE_AVG))
        ga, gb = rng.poisson(la), rng.poisson(lb)
        if ga > gb:
            return a
        if gb > ga:
            return b
        denom = power[a] + power[b]
        pa = power[a] / denom if denom > 0 else 0.5
        return a if rng.random() < pa else b

    for _ in range(runs):
        pts = base_pts.astype(float).copy()
        gf = base_gf.astype(float).copy()
        ga = base_ga.astype(float).copy()
        if has_fx:
            hg = rng.poisson(lam_home); ag = rng.poisson(lam_away)
            hw, dr, aw = hg > ag, hg == ag, ag > hg
            np.add.at(pts, fx_home, np.where(hw, 3, np.where(dr, 1, 0)))
            np.add.at(pts, fx_away, np.where(aw, 3, np.where(dr, 1, 0)))
            np.add.at(gf, fx_home, hg); np.add.at(ga, fx_home, ag)
            np.add.at(gf, fx_away, ag); np.add.at(ga, fx_away, hg)
        gd = gf - ga
        pts_sum += pts

        qualifiers: list[int] = []
        thirds: list[int] = []
        for members in groups.values():
            order = sorted(members, key=lambda t: (pts[t], gd[t], gf[t]), reverse=True)
            win[order[0]] += 1
            for t in order[:2]:
                top2[t] += 1; adv[t] += 1; qualifiers.append(t)
            if len(order) >= 3:
                thirds.append(order[2])
        for t in sorted(thirds, key=lambda t: (pts[t], gd[t], gf[t]), reverse=True)[:8]:
            adv[t] += 1; qualifiers.append(t)

        # --- mata-mata (só com 32 classificados) ---
        if len(qualifiers) != 32:
            continue
        bracket_runs += 1
        seeded = sorted(qualifiers, key=lambda t: power[t], reverse=True)
        cur = [seeded[s] for s in _SLOTS_32]
        for rc in (r16, qf, sf, fin):
            cur = [play(cur[2 * k], cur[2 * k + 1]) for k in range(len(cur) // 2)]
            for t in cur:
                rc[t] += 1
        champ[play(cur[0], cur[1])] += 1

    # --- saída: probabilidade de classificação por grupo ---
    out: dict[str, list[dict]] = {}
    for t, i in idx.items():
        out.setdefault(group_of[t], []).append({
            "team": t,
            "p_advance": round(100 * adv[i] / runs, 1),
            "p_top2": round(100 * top2[i] / runs, 1),
            "p_win_group": round(100 * win[i] / runs, 1),
            "exp_points": round(pts_sum[i] / runs, 1),
        })
    for g in out:
        out[g].sort(key=lambda r: r["p_advance"], reverse=True)

    # --- saída: probabilidade de título ---
    title: list[dict] = []
    if bracket_runs > 0:
        for t, i in idx.items():
            title.append({
                "team": t, "grp": group_of[t],
                "p_advance": round(100 * adv[i] / runs, 1),
                "p_r16": round(100 * r16[i] / runs, 1),
                "p_qf": round(100 * qf[i] / runs, 1),
                "p_sf": round(100 * sf[i] / runs, 1),
                "p_final": round(100 * fin[i] / runs, 1),
                "p_champion": round(100 * champ[i] / runs, 1),
            })
        title.sort(key=lambda r: (r["p_champion"], r["p_final"], r["p_sf"]), reverse=True)
        title = title[:16]

    return {"runs": runs, "groups": dict(sorted(out.items())), "title": title}


async def compute(pool: asyncpg.Pool) -> dict:
    standings = await pool.fetch(
        "SELECT team, grp, played, gf, ga, points FROM proj_standings")
    if not standings:
        return {"runs": 0, "groups": {}, "title": [], "note": "Sem dados de grupos ainda."}

    teams = [r["team"] for r in standings]
    idx = {t: i for i, t in enumerate(teams)}
    group_of = {r["team"]: r["grp"] for r in standings}
    base_pts = np.array([r["points"] for r in standings], dtype=float)
    base_gf = np.array([r["gf"] for r in standings], dtype=float)
    base_ga = np.array([r["ga"] for r in standings], dtype=float)

    att = np.zeros(len(teams)); dfn = np.zeros(len(teams)); power = np.zeros(len(teams))
    strength: dict[str, tuple[float, float]] = {}
    for r in standings:
        a, d = _strength(r["played"], r["gf"], r["ga"], RATING_OF.get(r["team"], 75))
        strength[r["team"]] = (a, d)
        att[idx[r["team"]]] = a
        dfn[idx[r["team"]]] = d
        power[idx[r["team"]]] = RATING_OF.get(r["team"], 75)

    remaining = await pool.fetch(
        "SELECT home, away FROM proj_match "
        "WHERE status <> 'finished' AND grp IS NOT NULL")
    fx_home, fx_away, lam_home, lam_away = [], [], [], []
    for r in remaining:
        if r["home"] not in idx or r["away"] not in idx:
            continue
        a_h, d_h = strength[r["home"]]; a_a, d_a = strength[r["away"]]
        fx_home.append(idx[r["home"]]); fx_away.append(idx[r["away"]])
        lam_home.append(max(0.15, min(5.0, _HOME_ADV * a_h * d_a / _LEAGUE_AVG)))
        lam_away.append(max(0.15, min(5.0, a_a * d_h / _LEAGUE_AVG)))

    return await asyncio.to_thread(
        _simulate, teams, group_of, base_pts, base_gf, base_ga,
        np.array(fx_home, dtype=int), np.array(fx_away, dtype=int),
        np.array(lam_home, dtype=float), np.array(lam_away, dtype=float),
        att, dfn, power, settings.monte_carlo_runs)
