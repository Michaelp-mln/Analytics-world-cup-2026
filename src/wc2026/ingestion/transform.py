"""Transformação (T do ETL): payloads da API-Football -> eventos de domínio."""
from __future__ import annotations

from wc2026.domain import events as E
from wc2026.domain.events import EventEnvelope

_LIVE = {"1H", "2H", "HT", "ET", "BT", "P", "LIVE", "INT", "SUSP"}
_FINISHED = {"FT", "AET", "PEN", "WO"}


def _stage_and_group(fixture: dict, group_map: dict[int, str]) -> tuple[str, str | None]:
    round_name = (fixture.get("league", {}).get("round") or "").lower()
    home_id = fixture.get("teams", {}).get("home", {}).get("id")
    if "group" in round_name:
        return "group", group_map.get(home_id)
    return "knockout", None


def fixture_lifecycle(fixture: dict, group_map: dict[int, str]) -> list[EventEnvelope]:
    """Eventos de ciclo de vida do jogo (agendado / iniciado / encerrado)."""
    fx = fixture.get("fixture", {})
    match_id = str(fx.get("id"))
    status = fx.get("status", {}).get("short", "NS")
    teams = fixture.get("teams", {})
    home = teams.get("home", {}).get("name", "?")
    away = teams.get("away", {}).get("name", "?")
    goals = fixture.get("goals", {})
    stage, grp = _stage_and_group(fixture, group_map)

    out = [E.match_scheduled(match_id, home, away, grp=grp, stage=stage,
                             kickoff=fx.get("date"))]
    if status in _LIVE or status in _FINISHED:
        out.append(E.match_started(match_id))
    if status in _FINISHED:
        out.append(E.match_finished(match_id, home, away,
                                    int(goals.get("home") or 0),
                                    int(goals.get("away") or 0)))
    return out


def parse_match_events(fixture: dict, raw_events: list[dict]) -> list[EventEnvelope]:
    """Gols e cartões a partir do endpoint /fixtures/events."""
    match_id = str(fixture.get("fixture", {}).get("id"))
    teams = fixture.get("teams", {})
    home = teams.get("home", {}).get("name", "?")
    away = teams.get("away", {}).get("name", "?")
    out: list[EventEnvelope] = []
    for ev in raw_events:
        etype = (ev.get("type") or "").lower()
        detail = (ev.get("detail") or "")
        team = ev.get("team", {}).get("name", "?")
        player = ev.get("player", {}).get("name") or "Desconhecido"
        minute = int((ev.get("time", {}).get("elapsed") or 0))
        if etype == "goal":
            if detail.lower() == "missed penalty":
                continue
            own_goal = detail.lower() == "own goal"
            # Gol contra: o gol conta para o ADVERSÁRIO do autor.
            scoring_team = (away if team == home else home) if own_goal else team
            assist = (ev.get("assist", {}) or {}).get("name")
            out.append(E.goal_scored(match_id, scoring_team, player, minute,
                                     assist=None if own_goal else assist,
                                     penalty="penalty" in detail.lower(),
                                     own_goal=own_goal))
        elif etype == "card":
            card = "red" if "red" in detail.lower() else "yellow"
            out.append(E.card_shown(match_id, team, player, minute, card))
    return out


def _possession_to_float(value) -> float:
    if isinstance(value, str) and value.endswith("%"):
        try:
            return float(value.rstrip("%"))
        except ValueError:
            return 0.0
    return float(value or 0)


def parse_statistics(fixture: dict, raw_stats: list[dict]) -> EventEnvelope | None:
    """Snapshot de posse/finalizações a partir de /fixtures/statistics."""
    if len(raw_stats) < 2:
        return None
    fx = fixture.get("fixture", {})
    match_id = str(fx.get("id"))
    minute = int((fx.get("status", {}).get("elapsed") or 0))
    teams = fixture.get("teams", {})
    home = teams.get("home", {}).get("name", "?")
    away = teams.get("away", {}).get("name", "?")

    def extract(side: dict) -> dict:
        d = {"poss": 0.0, "shots": 0, "sot": 0}
        for s in side.get("statistics", []):
            t = (s.get("type") or "").lower()
            v = s.get("value")
            if t == "ball possession":
                d["poss"] = _possession_to_float(v)
            elif t == "total shots":
                d["shots"] = int(v or 0)
            elif t == "shots on goal":
                d["sot"] = int(v or 0)
        return d

    # A ordem de raw_stats segue home, away (mesma ordem do fixture).
    h, a = extract(raw_stats[0]), extract(raw_stats[1])
    return E.stats_sampled(match_id, minute, home, away,
                           home_poss=h["poss"], away_poss=a["poss"],
                           home_shots=h["shots"], away_shots=a["shots"],
                           home_sot=h["sot"], away_sot=a["sot"])
