"""Self-test offline (sem Kafka/Postgres).

Valida a lógica pura do pipeline: idempotência dos eventos, transformação do
ETL e a simulação de Monte Carlo. Roda com:

    PYTHONPATH=src python scripts/selftest.py
"""
from __future__ import annotations

import sys
from collections import defaultdict

import numpy as np

from wc2026.domain import events as E
from wc2026.ingestion import transform as T
from wc2026.analytics import probability as P
from wc2026.analytics import winprob as W
from wc2026.analytics import golden_boot as GB
from wc2026.domain.teams import TEAMS, GROUP_OF, RATING_OF

ok = 0
fail = 0


def check(name: str, cond: bool) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  [PASS] {name}")
    else:
        fail += 1
        print(f"  [FAIL] {name}")


print("== 1. Eventos: idempotência e serialização ==")
g1 = E.goal_scored("m1", "Brasil", "BRA #9", 23, assist="BRA #10")
g2 = E.goal_scored("m1", "Brasil", "BRA #9", 23, assist="BRA #10")
g3 = E.goal_scored("m1", "Brasil", "BRA #9", 24)
check("mesmo gol -> mesmo event_id (idempotente)", g1.event_id == g2.event_id)
check("gol em minuto diferente -> id diferente", g1.event_id != g3.event_id)
roundtrip = E.EventEnvelope.from_bytes(g1.to_bytes())
check("round-trip envelope preserva tipo/payload",
      roundtrip.event_type == "GoalScored" and roundtrip.payload["player"] == "BRA #9")

print("\n== 2. ETL: transform de payloads da API-Football ==")
fixture = {
    "fixture": {"id": 777, "date": "2026-06-15T18:00:00+00:00",
                "status": {"short": "2H", "elapsed": 67}},
    "league": {"round": "Group Stage - 1"},
    "teams": {"home": {"id": 6, "name": "Brazil"}, "away": {"id": 2, "name": "Serbia"}},
    "goals": {"home": 2, "away": 0},
}
life = T.fixture_lifecycle(fixture, {6: "F"})
types = [e.event_type for e in life]
check("fixture ao vivo gera Scheduled+Started", "MatchScheduled" in types and "MatchStarted" in types)
check("grupo extraído do mapa de standings", life[0].payload["grp"] == "F")

raw_events = [
    {"time": {"elapsed": 30}, "team": {"name": "Brazil"},
     "player": {"name": "Neymar"}, "assist": {"name": "Vini"},
     "type": "Goal", "detail": "Normal Goal"},
    {"time": {"elapsed": 50}, "team": {"name": "Serbia"},
     "player": {"name": "Mitrovic"}, "type": "Goal", "detail": "Own Goal"},
    {"time": {"elapsed": 55}, "team": {"name": "Serbia"},
     "player": {"name": "X"}, "type": "Card", "detail": "Yellow Card"},
]
evs = T.parse_match_events(fixture, raw_events)
goals = [e for e in evs if e.event_type == "GoalScored"]
cards = [e for e in evs if e.event_type == "CardShown"]
check("2 gols + 1 cartão parseados", len(goals) == 2 and len(cards) == 1)
own = [g for g in goals if g.payload.get("own_goal")][0]
check("gol contra credita o adversário (Serbia->Brazil)", own.payload["team"] == "Brazil")

raw_stats = [
    {"team": {"name": "Brazil"}, "statistics": [
        {"type": "Ball Possession", "value": "61%"},
        {"type": "Total Shots", "value": 14}, {"type": "Shots on Goal", "value": 6}]},
    {"team": {"name": "Serbia"}, "statistics": [
        {"type": "Ball Possession", "value": "39%"},
        {"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 1}]},
]
st = T.parse_statistics(fixture, raw_stats)
check("posse parseada (61/39)", st.payload["home_poss"] == 61.0 and st.payload["away_poss"] == 39.0)
check("finalizações parseadas", st.payload["home_shots"] == 14 and st.payload["home_sot"] == 6)

print("\n== 3. Monte Carlo de classificação ==")
# Um grupo de 4, ainda sem jogos disputados (tudo a decidir).
teams = ["A1", "A2", "A3", "A4"]
group_of = {t: "A" for t in teams}
base = np.zeros(4)
# 6 jogos do grupo, todos restantes; A1 muito forte, A4 muito fraco.
fx_home = np.array([0, 2, 0, 3, 0, 1])
fx_away = np.array([1, 3, 2, 1, 3, 2])
lam_home = np.array([2.2, 1.0, 2.4, 0.6, 2.6, 1.5])
lam_away = np.array([0.6, 1.2, 0.6, 1.8, 0.5, 1.0])
_ones = np.ones(4)
_pw = np.array([4.0, 3.0, 2.0, 1.0])
res = P._simulate(teams, group_of, base, base.copy(), base.copy(),
                  fx_home, fx_away, lam_home, lam_away,
                  _ones, _ones.copy(), _pw, runs=3000)
rows = {r["team"]: r for r in res["groups"]["A"]}
total_adv = sum(r["p_advance"] for r in rows.values())
total_top2 = sum(r["p_top2"] for r in rows.values())
check("todas as probabilidades em [0,100]",
      all(0 <= r["p_advance"] <= 100 for r in rows.values()))
check("soma de p_top2 ~200% (2 vagas diretas por grupo)", 195 <= total_top2 <= 205)
# Grupo isolado: o único 3º colocado é sempre o "melhor terceiro" -> ~300%.
check("grupo isolado: 3º sempre classifica (soma p_advance ~300%)", 295 <= total_adv <= 305)
check("favorito (A1) avança mais que o azarão (A4)",
      rows["A1"]["p_advance"] > rows["A4"]["p_advance"])
print(f"    A1={rows['A1']['p_advance']}%  A2={rows['A2']['p_advance']}%  "
      f"A3={rows['A3']['p_advance']}%  A4={rows['A4']['p_advance']}%")

print("\n== 4. Probabilidade de título (bracket completo 32->campeão) ==")
names = [t[1] for t in TEAMS]
idx = {n: i for i, n in enumerate(names)}
group_of = {n: GROUP_OF[n] for n in names}
Tn = len(names)
z = np.zeros(Tn)
att = np.zeros(Tn); dfn = np.zeros(Tn); power = np.zeros(Tn)
for n in names:
    a, d = P._strength(0, 0, 0, RATING_OF[n])
    att[idx[n]] = a; dfn[idx[n]] = d; power[idx[n]] = RATING_OF[n]

gteams = defaultdict(list)
for n in names:
    gteams[group_of[n]].append(n)
fh, fa, lh, la = [], [], [], []
for mem in gteams.values():
    for i, j in [(0, 1), (2, 3), (0, 2), (1, 3), (0, 3), (1, 2)]:
        h, a = mem[i], mem[j]
        fh.append(idx[h]); fa.append(idx[a])
        lh.append(P._HOME_ADV * att[idx[h]] * dfn[idx[a]] / P._LEAGUE_AVG)
        la.append(att[idx[a]] * dfn[idx[h]] / P._LEAGUE_AVG)

res = P._simulate(names, group_of, z, z.copy(), z.copy(),
                  np.array(fh, int), np.array(fa, int),
                  np.array(lh, float), np.array(la, float),
                  att, dfn, power, runs=2000)
title = res["title"]
total_adv = sum(r["p_advance"] for grp in res["groups"].values() for r in grp)
champ_sum = sum(r["p_champion"] for r in title)
champ_desc = all(title[i]["p_champion"] >= title[i + 1]["p_champion"]
                 for i in range(len(title) - 1))
check("classificam 32 por torneio (soma p_advance ~3200%)", 3150 <= total_adv <= 3250)
check("título sem dupla contagem (top16 capta parte e <=100%)", 30 <= champ_sum <= 100.5)
check("ranking de título ordenado (p_champion decrescente)", champ_desc)
check("favorito ao título é uma seleção forte (rating>=84)",
      RATING_OF[title[0]["team"]] >= 84)
print("    Top 4 ao título: " + ", ".join(
    f"{r['team']} {r['p_champion']}%" for r in title[:4]))

print("\n== 5. Win probability ao vivo ==")
ph, pd, pa = W._result_probs(2, 0, 0.02, 0.02)
check("2-0 a ~1 min: mandante quase certo (>95%)", ph > 95)
ph, pd, pa = W._result_probs(0, 0, 0.0, 0.0)
check("0-0 sem tempo restante: empate 100%", pd == 100.0)
ph, pd, pa = W._result_probs(0, 0, 1.4, 1.1)
check("0-0 cedo: soma 100% e todos os resultados possíveis",
      abs(ph + pd + pa - 100) < 0.5 and ph > 0 and pd > 0 and pa > 0)

print("\n== 6. Corrida pela Chuteira de Ouro ==")
wins, proj = GB._simulate_race(np.array([5.0, 4.0, 3.0]),
                               np.array([0.1, 2.2, 0.2]), runs=4000)
check("prob de chuteira soma ~100%", 99.0 <= 100 * wins.sum() <= 101.0)
check("quem tem mais jogos pela frente ultrapassa o líder atual", wins[1] > wins[0])
wins2, _ = GB._simulate_race(np.array([5.0, 3.0, 2.0]), np.zeros(3), runs=1000)
check("sem jogos restantes: líder atual leva 100%", wins2[0] >= 0.995)
print(f"    prob chuteira: A={round(100*wins[0],1)}%  "
      f"B={round(100*wins[1],1)}%  C={round(100*wins[2],1)}%")

print(f"\nRESULTADO: {ok} passaram, {fail} falharam")
sys.exit(1 if fail else 0)
