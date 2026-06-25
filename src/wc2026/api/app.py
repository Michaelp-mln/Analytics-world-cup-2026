"""API de analytics (FastAPI): REST + WebSocket + dashboard.

Lê as projeções (read models) do Postgres e expõe os indicadores. Um
consumidor Kafka em segundo plano retransmite os eventos para os dashboards
conectados via WebSocket (atualização ao vivo).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from wc2026.analytics import golden_boot, probability, winprob
from wc2026.config import settings
from wc2026.domain.events import EventEnvelope
from wc2026.infra.db import make_pool
from wc2026.infra.kafka import make_consumer, make_producer
from wc2026.infra.migrate import ensure_schema
from wc2026.ingestion.manual_sim import ManualTournament

# Simulação manual ativa quando a fonte é o simulador em modo "manual".
MANUAL = settings.effective_source == "simulator" and settings.sim_mode == "manual"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("wc2026.api")

STATIC_DIR = Path(__file__).parent / "static"


# --- WebSocket broadcast ---------------------------------------------------
class Hub:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)

    async def broadcast(self, message: dict) -> None:
        dead = []
        for ws in self._clients:
            try:
                await ws.send_json(message)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


hub = Hub()


async def _kafka_to_ws(app: FastAPI) -> None:
    """Consome eventos e retransmite aos dashboards (group_id único = broadcast)."""
    consumer = await make_consumer(
        settings.kafka_topic_events,
        group_id=f"api-ws-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest")
    app.state.ws_consumer = consumer
    try:
        async for msg in consumer:
            env = EventEnvelope.from_bytes(msg.value)
            await hub.broadcast({
                "type": env.event_type,
                "match_id": env.aggregate_id,
                "payload": env.payload,
                "ts": env.occurred_at.isoformat(),
            })
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await make_pool()
    await ensure_schema(app.state.pool)
    app.state.prob_cache = {"ts": 0.0, "data": None}
    app.state.prob_lock = asyncio.Lock()
    app.state.ws_task = asyncio.create_task(_kafka_to_ws(app))

    app.state.producer = None
    app.state.tournament = None
    if MANUAL:
        app.state.producer = await make_producer()

        async def emit(env: EventEnvelope) -> None:
            await app.state.producer.send_and_wait(
                settings.kafka_topic_events, value=env.to_bytes(),
                key=env.aggregate_id.encode("utf-8"))

        app.state.emit = emit
        app.state.tournament = ManualTournament()
        await app.state.tournament.load(app.state.pool)
        await app.state.tournament.ensure_scheduled(emit, app.state.pool)
        log.info("Simulação MANUAL habilitada (controlada pelo dashboard).")

    log.info("API pronta.")
    yield
    app.state.ws_task.cancel()
    if app.state.producer:
        await app.state.producer.stop()
    await app.state.pool.close()


app = FastAPI(title="WC 2026 Analytics", version="1.0.0", lifespan=lifespan)


def pool():
    return app.state.pool


# --- REST ------------------------------------------------------------------
@app.get("/api/health")
async def health():
    try:
        await pool().fetchval("SELECT 1")
        return {"status": "ok", "source": settings.effective_source}
    except Exception as exc:  # noqa: BLE001
        return {"status": "degraded", "error": str(exc)}


@app.get("/api/summary")
async def summary():
    row = await pool().fetchrow(
        """
        SELECT
          COUNT(*) FILTER (WHERE status='finished')              AS finished,
          COUNT(*) FILTER (WHERE status='live')                  AS live,
          COUNT(*) FILTER (WHERE status='scheduled')             AS scheduled,
          COALESCE(SUM(home_score+away_score)
                   FILTER (WHERE status='finished'),0)           AS goals
        FROM proj_match
        """)
    finished = row["finished"] or 0
    goals = row["goals"] or 0
    top = await pool().fetchrow(
        "SELECT player, team, goals FROM proj_scorers ORDER BY goals DESC, assists DESC LIMIT 1")
    return {
        "matches_finished": finished,
        "matches_live": row["live"] or 0,
        "matches_scheduled": row["scheduled"] or 0,
        "total_goals": goals,
        "avg_goals_per_match": round(goals / finished, 2) if finished else 0.0,
        "top_scorer": dict(top) if top else None,
        "source": settings.effective_source,
    }


@app.get("/api/scorers")
async def scorers(limit: int = 15):
    rows = await pool().fetch(
        """
        SELECT player, team, goals, assists, penalties
        FROM proj_scorers WHERE goals > 0
        ORDER BY goals DESC, assists DESC, player ASC LIMIT $1
        """, limit)
    return [dict(r) for r in rows]


@app.get("/api/teams")
async def teams():
    rows = await pool().fetch(
        """
        SELECT s.team, s.grp, s.played, s.won, s.drawn, s.lost,
               s.gf, s.ga, s.points, s.clean_sheets,
               ROUND(COALESCE(ms.avg_poss, 0), 1)        AS avg_possession,
               COALESCE(ms.shots, 0)                     AS shots,
               COALESCE(ms.sot, 0)                       AS shots_on_target,
               COALESCE(d.yellow, 0)                     AS yellow,
               COALESCE(d.red, 0)                        AS red,
               CASE WHEN s.played > 0
                    THEN ROUND(s.gf::numeric / s.played, 2) ELSE 0 END AS avg_goals_for,
               CASE WHEN s.played > 0
                    THEN ROUND(s.ga::numeric / s.played, 2) ELSE 0 END AS avg_goals_against,
               CASE WHEN COALESCE(ms.shots, 0) > 0
                    THEN ROUND(s.gf::numeric / ms.shots * 100, 1) ELSE 0 END AS shot_conversion,
               CASE WHEN COALESCE(ms.shots, 0) > 0
                    THEN ROUND(ms.sot::numeric / ms.shots * 100, 1) ELSE 0 END AS sot_accuracy
        FROM proj_standings s
        LEFT JOIN (
            SELECT team, AVG(possession) AS avg_poss,
                   SUM(shots) AS shots, SUM(shots_on_target) AS sot
            FROM proj_match_stats GROUP BY team
        ) ms ON ms.team = s.team
        LEFT JOIN proj_discipline d ON d.team = s.team
        ORDER BY s.grp, s.points DESC, (s.gf - s.ga) DESC
        """)
    return [dict(r) for r in rows]


@app.get("/api/standings")
async def standings():
    rows = await pool().fetch(
        """
        SELECT team, grp, played, won, drawn, lost, gf, ga,
               (gf - ga) AS gd, points
        FROM proj_standings
        ORDER BY grp, points DESC, (gf - ga) DESC, gf DESC, team ASC
        """)
    grouped: dict[str, list[dict]] = {}
    for r in rows:
        grouped.setdefault(r["grp"], []).append(dict(r))
    return grouped


@app.get("/api/matches")
async def matches(limit: int = 60):
    rows = await pool().fetch(
        """
        SELECT match_id, grp, stage, home, away, home_score, away_score,
               home_possession, away_possession, minute, status, kickoff
        FROM proj_match
        ORDER BY
          CASE status WHEN 'live' THEN 0 WHEN 'finished' THEN 1 ELSE 2 END,
          updated_at DESC, kickoff NULLS LAST
        LIMIT $1
        """, limit)
    return [dict(r) for r in rows]


_BUCKET_ORDER = ["1-15", "16-30", "31-45", "46-60", "61-75", "76-90", "90+"]


@app.get("/api/goal-timing")
async def goal_timing():
    rows = await pool().fetch("SELECT bucket, goals FROM proj_goal_timing")
    by = {r["bucket"]: r["goals"] for r in rows}
    return [{"bucket": b, "goals": by.get(b, 0)} for b in _BUCKET_ORDER]


@app.get("/api/fairplay")
async def fairplay():
    rows = await pool().fetch(
        """
        SELECT s.team, s.grp,
               COALESCE(d.yellow, 0) AS yellow,
               COALESCE(d.red, 0)    AS red,
               COALESCE(d.yellow, 0) + COALESCE(d.red, 0) * 3 AS fair_play_points
        FROM proj_standings s
        LEFT JOIN proj_discipline d ON d.team = s.team
        ORDER BY fair_play_points ASC, s.team ASC
        """)
    return [dict(r) for r in rows]


@app.get("/api/winprob")
async def winprob_live():
    return await winprob.compute_live(pool())


async def _cached_probabilities():
    cache = app.state.prob_cache
    if cache["data"] is not None and time.time() - cache["ts"] < 20:
        return cache["data"]
    async with app.state.prob_lock:
        if cache["data"] is not None and time.time() - cache["ts"] < 20:
            return cache["data"]
        data = await probability.compute(pool())
        cache["data"], cache["ts"] = data, time.time()
        return data


@app.get("/api/probabilities")
async def probabilities():
    return await _cached_probabilities()


@app.get("/api/golden-boot")
async def golden_boot_race():
    prob = await _cached_probabilities()
    return await golden_boot.compute(pool(), prob.get("title", []))


# --- Controle da simulação manual -----------------------------------------
_KO_LABEL = {"R32": "32-avos", "R16": "Oitavas", "QF": "Quartas",
             "SF": "Semis", "3P": "3º lugar", "FINAL": "Final"}
_KO_ORDER = {"R32": 0, "R16": 1, "QF": 2, "SF": 3, "3P": 4, "FINAL": 5}


@app.get("/api/sim/state")
async def sim_state():
    t = app.state.tournament
    if not t:
        return {"enabled": False}
    return {"enabled": True, **t.state()}


@app.post("/api/sim/next-round")
async def sim_next_round():
    t = app.state.tournament
    if not t:
        return {"enabled": False, "error": "Simulação manual desativada."}
    return await t.next_round(app.state.emit, app.state.pool)


@app.post("/api/sim/reset")
async def sim_reset():
    t = app.state.tournament
    if not t:
        return {"enabled": False}
    state = await t.reset(app.state.emit, app.state.pool)
    app.state.prob_cache = {"ts": 0.0, "data": None}
    return {"enabled": True, "reset": True, **state}


@app.get("/api/bracket")
async def bracket():
    rows = await pool().fetch(
        """
        SELECT match_id, home, away, home_score, away_score, status,
               winner, pen_home, pen_away
        FROM proj_match WHERE stage = 'knockout'
        """)
    by: dict[str, list[dict]] = {}
    for r in rows:
        parts = r["match_id"].split("-")          # SIM-KO-<LABEL>-<slot>
        label = parts[2] if len(parts) >= 4 else "?"
        by.setdefault(label, []).append(dict(r))
    out = []
    for label in sorted(by, key=lambda x: _KO_ORDER.get(x, 9)):
        matches = sorted(by[label], key=lambda m: m["match_id"])
        out.append({"round": label, "label": _KO_LABEL.get(label, label),
                    "matches": matches})
    return out


# --- WebSocket -------------------------------------------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()   # mantém a conexão; ignoramos o conteúdo
    except WebSocketDisconnect:
        hub.disconnect(ws)
    except Exception:  # noqa: BLE001
        hub.disconnect(ws)


# --- Dashboard estático ----------------------------------------------------
@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def run() -> None:
    import uvicorn
    uvicorn.run("wc2026.api.app:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run()
