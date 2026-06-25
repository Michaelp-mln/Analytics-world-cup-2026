-- ======================================================================
-- WC 2026 Analytics — schema
--
--  * events          -> EVENT STORE (append-only, fonte da verdade)
--  * proj_*          -> READ MODELS / projeções (reconstruíveis a partir
--                       do event store via `python -m wc2026.processing.projectors --rebuild`)
-- ======================================================================

-- ---------------------------------------------------------------------
-- EVENT STORE (Event Sourcing)
-- Log imutável e ordenado de tudo que aconteceu nas partidas.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    global_seq      BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,        -- idempotência (uuid5 determinístico)
    aggregate_type  TEXT        NOT NULL DEFAULT 'match',
    aggregate_id    TEXT        NOT NULL,               -- match_id
    event_type      TEXT        NOT NULL,
    version         INT         NOT NULL,               -- versão sequencial por agregado
    payload         JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, version)
);
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events (aggregate_id, version);
CREATE INDEX IF NOT EXISTS idx_events_type      ON events (event_type);

-- ---------------------------------------------------------------------
-- Guarda de idempotência para os projetores (entrega "at-least-once")
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proj_processed (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- READ MODEL: partidas (estado atual de cada jogo)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proj_match (
    match_id        TEXT PRIMARY KEY,
    grp             TEXT,                  -- grupo (A..L) ou NULL no mata-mata
    stage           TEXT,                  -- 'group' | 'knockout'
    home            TEXT NOT NULL,
    away            TEXT NOT NULL,
    home_score      INT  NOT NULL DEFAULT 0,
    away_score      INT  NOT NULL DEFAULT 0,
    home_possession NUMERIC(5,2),
    away_possession NUMERIC(5,2),
    minute          INT  NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'scheduled',   -- scheduled|live|finished
    kickoff         TIMESTAMPTZ,
    winner          TEXT,                  -- mata-mata: quem avançou
    pen_home        INT,                   -- pênaltis (se houve)
    pen_away        INT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_match_status ON proj_match (status);
CREATE INDEX IF NOT EXISTS idx_match_group  ON proj_match (grp);

-- ---------------------------------------------------------------------
-- READ MODEL: artilheiros / assistências
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proj_scorers (
    player   TEXT NOT NULL,
    team     TEXT NOT NULL,
    goals    INT  NOT NULL DEFAULT 0,
    assists  INT  NOT NULL DEFAULT 0,
    penalties INT NOT NULL DEFAULT 0,
    PRIMARY KEY (player, team)
);
CREATE INDEX IF NOT EXISTS idx_scorers_goals ON proj_scorers (goals DESC);

-- ---------------------------------------------------------------------
-- READ MODEL: snapshot CUMULATIVO de estatísticas por (partida, time).
-- Guarda o último total conhecido de posse/finalizações de cada time em
-- cada jogo. As médias por seleção são derivadas por agregação na leitura
-- (AVG de posse, SUM de finalizações) — sem risco de soma duplicada.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proj_match_stats (
    match_id        TEXT NOT NULL,
    team            TEXT NOT NULL,
    grp             TEXT,
    possession      NUMERIC(5,2) NOT NULL DEFAULT 0,
    shots           INT NOT NULL DEFAULT 0,
    shots_on_target INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, team)
);
CREATE INDEX IF NOT EXISTS idx_match_stats_team ON proj_match_stats (team);

-- READ MODEL: disciplina (cartões) agregada por seleção (incremental).
CREATE TABLE IF NOT EXISTS proj_discipline (
    team    TEXT PRIMARY KEY,
    grp     TEXT,
    yellow  INT NOT NULL DEFAULT 0,
    red     INT NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------
-- READ MODEL: classificação dos grupos
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proj_standings (
    team         TEXT PRIMARY KEY,
    grp          TEXT NOT NULL,
    played       INT NOT NULL DEFAULT 0,
    won          INT NOT NULL DEFAULT 0,
    drawn        INT NOT NULL DEFAULT 0,
    lost         INT NOT NULL DEFAULT 0,
    gf           INT NOT NULL DEFAULT 0,
    ga           INT NOT NULL DEFAULT 0,
    points       INT NOT NULL DEFAULT 0,
    clean_sheets INT NOT NULL DEFAULT 0       -- jogos sem sofrer gol
);
CREATE INDEX IF NOT EXISTS idx_standings_group ON proj_standings (grp);

-- ---------------------------------------------------------------------
-- READ MODEL: distribuição de gols por intervalo de tempo do jogo.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proj_goal_timing (
    bucket TEXT PRIMARY KEY,          -- '1-15','16-30',...,'76-90','90+'
    goals  INT NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------
-- Estado do simulador manual (controlado pelo botão do dashboard).
-- Linha única (id=1): rodadas de grupo jogadas, fase de mata-mata e o
-- estado do chaveamento (classificados, vencedores e perdedores).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sim_state (
    id           INT PRIMARY KEY DEFAULT 1,
    group_played INT NOT NULL DEFAULT 0,
    ko_index     INT NOT NULL DEFAULT 0,
    state        JSONB NOT NULL DEFAULT '{}'::jsonb
);
