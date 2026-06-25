"""Adaptador (E do ETL) para a API-Football v3.

Docs: https://www.api-football.com/documentation-v3
Endpoints usados:
    GET /fixtures?league=1&season=2026&live=all   -> jogos ao vivo
    GET /fixtures?league=1&season=2026             -> todos os jogos
    GET /fixtures/events?fixture={id}              -> gols, cartões, etc.
    GET /fixtures/statistics?fixture={id}          -> posse, finalizações...
    GET /standings?league=1&season=2026            -> mapa time -> grupo
"""
from __future__ import annotations

import logging

import httpx

from wc2026.config import settings

log = logging.getLogger("wc2026.api_football")


class APIFootballClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.api_football_base_url,
            headers={"x-apisports-key": settings.api_football_key},
            timeout=20.0,
        )
        self.league = settings.api_football_league_id
        self.season = settings.api_football_season

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict) -> list[dict]:
        resp = await self._client.get(path, params=params)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            log.warning("API-Football retornou erros em %s: %s", path, data["errors"])
        return data.get("response", [])

    async def live_fixtures(self) -> list[dict]:
        return await self._get("/fixtures",
                               {"league": self.league, "season": self.season, "live": "all"})

    async def all_fixtures(self) -> list[dict]:
        return await self._get("/fixtures",
                               {"league": self.league, "season": self.season})

    async def fixture_events(self, fixture_id: int) -> list[dict]:
        return await self._get("/fixtures/events", {"fixture": fixture_id})

    async def fixture_statistics(self, fixture_id: int) -> list[dict]:
        return await self._get("/fixtures/statistics", {"fixture": fixture_id})

    async def standings(self) -> list[dict]:
        return await self._get("/standings",
                               {"league": self.league, "season": self.season})

    async def team_group_map(self) -> dict[int, str]:
        """Mapa team_id -> letra do grupo, derivado das classificações."""
        mapping: dict[int, str] = {}
        for league in await self.standings():
            for group in league.get("league", {}).get("standings", []):
                for row in group:
                    grp_name = row.get("group", "")          # ex.: "Group A"
                    letter = grp_name.replace("Group", "").strip() or None
                    team_id = row.get("team", {}).get("id")
                    if team_id is not None and letter:
                        mapping[team_id] = letter
        return mapping
