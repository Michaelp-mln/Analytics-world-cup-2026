"""Configuração central, carregada de variáveis de ambiente / .env."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Ingestão
    ingestion_source: str = "api"          # "api" | "simulator"
    sim_mode: str = "manual"               # "auto" (autônomo) | "manual" (por botão)
    poll_interval_seconds: float = 20.0
    sim_seconds_per_minute: float = 0.4

    # API-Football
    api_football_key: str = ""
    api_football_base_url: str = "https://v3.football.api-sports.io"
    api_football_league_id: int = 1        # 1 = World Cup na API-Football
    api_football_season: int = 2026

    # Analytics
    monte_carlo_runs: int = 10_000

    # Infra
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_events: str = "match.events"
    postgres_dsn: str = "postgresql://wc2026:wc2026@localhost:5432/wc2026"

    @property
    def effective_source(self) -> str:
        """Cai para o simulador se 'api' for pedido sem chave configurada."""
        if self.ingestion_source == "api" and not self.api_football_key:
            return "simulator"
        return self.ingestion_source


settings = Settings()
