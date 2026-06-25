# ⚽ Copa do Mundo FIFA 2026 — Sistema de Estatísticas & Analytics

Pipeline de dados **orientado a eventos** que consome dados de cada partida da
Copa 2026, processa de forma **assíncrona** e gera estatísticas em tempo real:
artilheiros, posse de bola média, média de gols, classificação dos grupos e
**probabilidade de classificação** (simulação de Monte Carlo) — tudo num
**dashboard ao vivo**.

> Construído com **ETL · Apache Kafka · Event Sourcing · processamento
> assíncrono · projeções (CQRS) · dashboards**.

---

## 🧱 Arquitetura

```
   ┌─────────────────────┐
   │  API-Football (real) │   ou   Simulador de partidas (fallback)
   └──────────┬──────────┘
              │  ETL: Extract + Transform  (ingestion/)
              ▼
        ┌───────────┐
        │  KAFKA    │  topic: match.events   (particionado por match_id)
        └─────┬─────┘
              │  (2 grupos de consumidores independentes, assíncronos)
      ┌───────┴─────────────┐
      ▼                     ▼
┌──────────────┐    ┌──────────────────┐
│ EVENT STORE  │    │   PROJETORES      │
│ (append-only)│    │  read models /    │
│ Postgres     │    │  CQRS  (proj_*)   │
│ event sourcing│   └─────────┬────────┘
└──────┬───────┘              │
       │  replay / rebuild    ▼
       └────────────►  ┌──────────────┐     WebSocket (ao vivo)
                       │  FastAPI     │◄──── Kafka consumer ──┐
                       │  REST + WS   │                       │
                       └──────┬───────┘                       │
                              ▼                               │
                        Dashboard (HTML + Chart.js) ──────────┘
```

### Por que essas tecnologias

| Requisito | Implementação |
|---|---|
| **ETL** | `ingestion/` extrai da API-Football, transforma payloads em eventos de domínio |
| **Mensageria (Kafka)** | `infra/kafka.py` — broker KRaft; eventos particionados por `match_id` (ordem por partida) |
| **Event Sourcing** | `events` é um log append-only e imutável; `event_id` determinístico (uuid5) = idempotência |
| **Processamento assíncrono** | `asyncio` + `aiokafka` + `asyncpg`; consumidores independentes; Monte Carlo em thread |
| **CQRS / projeções** | tabelas `proj_*` reconstruíveis a partir do event store (`--rebuild`) |
| **Dashboards** | FastAPI serve REST + WebSocket; SPA com Chart.js |

---

## 🚀 Como rodar (Docker Compose)

Pré-requisitos: **Docker Desktop**.

```bash
# 1. configurar ambiente
cp .env.example .env

# 2. subir tudo (broker, banco, ingestão, consumidores, API)
docker compose up --build
```

Acesse o dashboard em **http://localhost:8000**.

Sem chave de API, o sistema sobe no **modo simulador** automaticamente e já
começa a gerar um torneio sintético completo — o dashboard se popula em segundos.

### Serviços

| Serviço | Papel | Porta |
|---|---|---|
| `kafka` | broker de eventos (KRaft) | 9092 |
| `postgres` | event store + read models | 5432 |
| `ingestion` | ETL → Kafka (produtor) | — |
| `eventstore-writer` | Kafka → event store | — |
| `projector` | Kafka → projeções | — |
| `api` | FastAPI + dashboard | 8000 |

---

## 🌐 Usar dados REAIS (API-Football)

1. Crie uma conta em <https://www.api-football.com/> e copie sua chave.
2. No `.env`:
   ```env
   INGESTION_SOURCE=api
   API_FOOTBALL_KEY=sua_chave_aqui
   ```
3. `docker compose up --build`

A ingestão passa a varrer `league=1` (Copa do Mundo), `season=2026`: jogos ao
vivo, eventos (gols/cartões), estatísticas (posse/finalizações) e classificação.

> ⚠️ O plano gratuito da API-Football tem limite de ~100 requisições/dia.
> Ajuste `POLL_INTERVAL_SECONDS` para não estourar a cota. Se a chave estiver
> ausente, o sistema cai para o simulador (com aviso no log).

---

## 📡 API REST

| Endpoint | Descrição |
|---|---|
| `GET /api/summary` | KPIs: total de gols, média/jogo, ao vivo, artilheiro |
| `GET /api/scorers?limit=15` | Artilheiros (gols, assistências, pênaltis) |
| `GET /api/teams` | Estatísticas por seleção (posse média, finalizações, médias) |
| `GET /api/standings` | Classificação por grupo |
| `GET /api/matches` | Partidas (ao vivo, agendadas, encerradas) |
| `GET /api/probabilities` | Probabilidade de classificação (Monte Carlo) |
| `GET /api/health` | Saúde do serviço |
| `WS  /ws` | Stream de eventos ao vivo |

---

## 🔁 Event Sourcing na prática

O event store é a **fonte da verdade**. As projeções são derivadas e podem ser
**reconstruídas do zero** a qualquer momento (ex.: após mudar a lógica de uma
estatística):

```bash
docker compose run --rm projector python -m wc2026.processing.projectors --rebuild
```

Garantias:
- **Idempotência na escrita**: `event_id` determinístico → o mesmo lance nunca
  duplica, mesmo reprocessando a API.
- **Idempotência na projeção**: guarda `proj_processed` torna o consumo seguro
  sob entrega *at-least-once* do Kafka.
- **Ordem por partida**: chave de partição = `match_id`.

---

## 🎲 Probabilidade de classificação

Formato 2026: 12 grupos de 4 → avançam 1º, 2º e os **8 melhores 3º** (32 times).
Para cada jogo restante, os gols de cada lado são amostrados de uma **Poisson**
cujo λ vem da força de ataque/defesa observada (gols por jogo), suavizada por um
prior de rating. Rodando milhares de cenários (`MONTE_CARLO_RUNS`), estimamos a
chance de cada seleção avançar. Jogos encerrados entram como pontuação fixa.

---

## 🗂️ Estrutura

```
src/wc2026/
├── config.py              # configurações (.env)
├── domain/
│   ├── events.py          # eventos de domínio + idempotência (uuid5)
│   └── teams.py           # 48 seleções (grupos/ratings ilustrativos p/ simulador)
├── infra/
│   ├── kafka.py           # produtor/consumidor aiokafka (com retry)
│   ├── db.py              # pool asyncpg
│   └── eventstore.py      # append/read do log de eventos
├── ingestion/             # ETL (produtor Kafka)
│   ├── api_football.py     # adaptador da API real
│   ├── transform.py        # payloads -> eventos de domínio
│   ├── simulator.py        # torneio sintético (fallback)
│   └── service.py          # loop principal de ingestão
├── processing/            # consumidores assíncronos
│   ├── eventstore_consumer.py
│   └── projectors.py       # eventos -> read models (+ rebuild)
├── analytics/
│   └── probability.py      # Monte Carlo de classificação
└── api/
    ├── app.py              # FastAPI: REST + WebSocket
    └── static/index.html   # dashboard
```

---

## ✅ Validação offline (sem infra)

A lógica pura (idempotência de eventos, ETL e Monte Carlo) tem um self-test que
roda sem Kafka/Postgres:

```bash
pip install pydantic pydantic-settings numpy asyncpg
PYTHONPATH=src python scripts/selftest.py
```

---

## 🛠️ Rodar localmente sem Docker (opcional)

Requer Python 3.12+, um Kafka e um Postgres acessíveis. Ajuste
`KAFKA_BOOTSTRAP_SERVERS` e `POSTGRES_DSN` no `.env` e aplique `sql/init.sql`.

```bash
pip install -r requirements.txt
export PYTHONPATH=src
python -m wc2026.ingestion.service          # terminal 1
python -m wc2026.processing.eventstore_consumer  # terminal 2
python -m wc2026.processing.projectors      # terminal 3
uvicorn wc2026.api.app:app --port 8000      # terminal 4
```
