"""WC 2026 Analytics — pipeline de estatísticas em tempo real.

Arquitetura (event-driven):

    API-Football / Simulador
            │  (ETL: extract + transform)
            ▼
        Kafka  (topic: match.events)
            │
      ┌─────┴──────────────┐
      ▼                    ▼
 event store         projetores (read models)
 (event sourcing)    artilheiros, posse, médias,
                     classificação, probabilidades
                          │
                          ▼
                  FastAPI (REST + WebSocket) + dashboard
"""

__version__ = "1.0.0"
