"""Seleções da Copa do Mundo FIFA 2026.

IMPORTANTE: os grupos e as forças (`rating`) abaixo são ILUSTRATIVOS — servem
apenas para alimentar o SIMULADOR quando não há chave da API-Football. Em modo
`api`, grupos, jogos e estatísticas vêm dos dados reais da API. A Copa de 2026
tem 48 seleções em 12 grupos (A–L) de 4; avançam os 2 primeiros de cada grupo
mais os 8 melhores terceiros colocados (32 times no mata-mata).
"""
from __future__ import annotations

# rating ~ força relativa (usada como prior de ataque/defesa). Faixa ~60–92.
# (team_code, nome, grupo, rating)
TEAMS: list[tuple[str, str, str, int]] = [
    # Grupo A
    ("MEX", "México", "A", 78), ("CRO", "Croácia", "A", 82),
    ("NGA", "Nigéria", "A", 75), ("KSA", "Arábia Saudita", "A", 66),
    # Grupo B
    ("CAN", "Canadá", "B", 74), ("BEL", "Bélgica", "B", 84),
    ("EGY", "Egito", "B", 72), ("JPN", "Japão", "B", 79),
    # Grupo C
    ("USA", "Estados Unidos", "C", 77), ("URU", "Uruguai", "C", 83),
    ("KOR", "Coreia do Sul", "C", 74), ("GHA", "Gana", "C", 71),
    # Grupo D
    ("ARG", "Argentina", "D", 91), ("AUS", "Austrália", "D", 70),
    ("POL", "Polônia", "D", 76), ("CIV", "Costa do Marfim", "D", 73),
    # Grupo E
    ("FRA", "França", "E", 90), ("ECU", "Equador", "E", 74),
    ("SEN", "Senegal", "E", 78), ("IRN", "Irã", "E", 70),
    # Grupo F
    ("BRA", "Brasil", "F", 89), ("SUI", "Suíça", "F", 78),
    ("CMR", "Camarões", "F", 72), ("QAT", "Catar", "F", 64),
    # Grupo G
    ("ENG", "Inglaterra", "G", 88), ("SRB", "Sérvia", "G", 76),
    ("MAR", "Marrocos", "G", 80), ("PAN", "Panamá", "G", 65),
    # Grupo H
    ("ESP", "Espanha", "H", 89), ("DEN", "Dinamarca", "H", 79),
    ("PER", "Peru", "H", 70), ("UZB", "Uzbequistão", "H", 66),
    # Grupo I
    ("POR", "Portugal", "I", 88), ("USA2", "Costa Rica", "I", 69),
    ("TUN", "Tunísia", "I", 71), ("AUT", "Áustria", "I", 77),
    # Grupo J
    ("GER", "Alemanha", "J", 87), ("COL", "Colômbia", "J", 81),
    ("SCO", "Escócia", "J", 73), ("JOR", "Jordânia", "J", 63),
    # Grupo K
    ("NED", "Holanda", "K", 85), ("MEX2", "Chile", "K", 72),
    ("ALG", "Argélia", "K", 74), ("NZL", "Nova Zelândia", "K", 62),
    # Grupo L
    ("ITA", "Itália", "L", 84), ("PAR", "Paraguai", "L", 71),
    ("NOR", "Noruega", "L", 80), ("RSA", "África do Sul", "L", 69),
]

GROUPS: list[str] = list("ABCDEFGHIJKL")

# Lookups práticos
BY_NAME: dict[str, dict] = {
    name: {"code": code, "name": name, "grp": grp, "rating": rating}
    for code, name, grp, rating in TEAMS
}
GROUP_OF: dict[str, str] = {name: grp for _, name, grp, _ in TEAMS}
RATING_OF: dict[str, int] = {name: rating for _, name, _, rating in TEAMS}


def teams_in_group(grp: str) -> list[str]:
    return [name for _, name, g, _ in TEAMS if g == grp]
