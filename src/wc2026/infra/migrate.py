"""Migrações idempotentes — garantem o schema novo em bancos já existentes.

Roda no startup do projetor e da API, então adicionar projeções não exige
recriar o volume do Postgres.
"""
from __future__ import annotations

import logging

import asyncpg

log = logging.getLogger("wc2026.migrate")

_DDL = [
    "ALTER TABLE proj_standings ADD COLUMN IF NOT EXISTS clean_sheets INT NOT NULL DEFAULT 0",
    "ALTER TABLE proj_match ADD COLUMN IF NOT EXISTS winner TEXT",
    "ALTER TABLE proj_match ADD COLUMN IF NOT EXISTS pen_home INT",
    "ALTER TABLE proj_match ADD COLUMN IF NOT EXISTS pen_away INT",
    """CREATE TABLE IF NOT EXISTS proj_goal_timing (
           bucket TEXT PRIMARY KEY,
           goals  INT NOT NULL DEFAULT 0
       )""",
    """CREATE TABLE IF NOT EXISTS sim_state (
           id           INT PRIMARY KEY DEFAULT 1,
           group_played INT NOT NULL DEFAULT 0,
           ko_index     INT NOT NULL DEFAULT 0,
           state        JSONB NOT NULL DEFAULT '{}'::jsonb
       )""",
]


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for stmt in _DDL:
            await conn.execute(stmt)
    log.info("Schema verificado/migrado.")
