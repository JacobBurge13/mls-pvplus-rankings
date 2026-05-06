from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="MLS 2026 PV+ Table",
    page_icon=":soccer:",
    layout="wide",
)


BG = "#0b1018"
PANEL = "#131923"
GRID = "#313846"
TEXT = "#f5f7fb"
MUTED = "#8f98a8"
GOLD = "#7fc8ff"
BLUE = "#5d86c9"
GREEN = "#76d37c"
RED = "#ef6b6b"
GREY = "#b8bec9"

TEAM_LOGO_IDS = {
    "Atlanta United": "26666",
    "Austin FC": "29664",
    "Charlotte FC": "30105",
    "Chicago": "1118",
    "Colorado": "1120",
    "Columbus": "1113",
    "DC United": "1119",
    "FC Cincinnati": "24949",
    "FC Dallas": "2948",
    "Houston": "3624",
    "Inter Miami CF": "28925",
    "Kansas City": "1116",
    "L.A. Galaxy": "1117",
    "Los Angeles FC": "27482",
    "Minnesota United": "9293",
    "Montreal": "11135",
    "Nashville SC": "27497",
    "New England": "1114",
    "New York": "1121",
    "New York City FC": "19584",
    "Orlando City": "10221",
    "Philadelphia": "8586",
    "Portland": "11133",
    "Salt Lake": "2947",
    "San Diego FC": "32064",
    "San Jose": "1122",
    "Seattle": "5973",
    "St. Louis City": "30664",
    "Toronto": "4186",
    "Vancouver": "11134",
}


@dataclass(frozen=True)
class DbConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: int


def secret_or_env(secret_key: str, env_key: str) -> str | None:
    if secret_key in st.secrets:
        value = st.secrets[secret_key]
        if value is not None and str(value).strip():
            return str(value)
    value = os.getenv(env_key)
    if value is not None and value.strip():
        return value
    return None


def db_config() -> DbConfig:
    dbname = secret_or_env("SUPABASE_DB_NAME", "SUPABASE_DB_NAME")
    user = secret_or_env("SUPABASE_DB_USER", "SUPABASE_DB_USER")
    password = secret_or_env("SUPABASE_DB_PASSWORD", "SUPABASE_DB_PASSWORD")
    host = secret_or_env("SUPABASE_DB_HOST", "SUPABASE_DB_HOST")
    port = secret_or_env("SUPABASE_DB_PORT", "SUPABASE_DB_PORT")

    missing = [
        name
        for name, value in {
            "SUPABASE_DB_NAME": dbname,
            "SUPABASE_DB_USER": user,
            "SUPABASE_DB_PASSWORD": password,
            "SUPABASE_DB_HOST": host,
            "SUPABASE_DB_PORT": port,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Missing database secrets: "
            + ", ".join(missing)
            + ". Add them to Streamlit secrets or local environment variables."
        )

    return DbConfig(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=int(port),
    )


def inject_styles() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                radial-gradient(circle at top left, rgba(93,134,201,0.14), transparent 28%),
                radial-gradient(circle at top right, rgba(199,161,90,0.10), transparent 22%),
                linear-gradient(180deg, {BG} 0%, #090d14 100%);
            color: {TEXT};
        }}
        .block-container {{
            padding-top: 5.5rem;
            padding-bottom: 2rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, rgba(19,25,35,0.98), rgba(11,16,24,0.98));
            border-right: 1px solid rgba(255,255,255,0.06);
        }}
        .pv-kicker {{
            color: {GOLD};
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 1rem;
            font-weight: 800;
            margin-bottom: 0.35rem;
        }}
        .pv-subtitle {{
            color: {MUTED};
            font-size: 0.95rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            font-weight: 700;
            margin-bottom: 1.25rem;
        }}
        .front-title {{
            color: {TEXT};
            font-size: 3.6rem;
            font-weight: 900;
            line-height: 1.02;
            margin: 0 0 0.35rem 0;
        }}
        .front-subtitle {{
            color: {GOLD};
            font-size: 1.55rem;
            font-weight: 800;
            line-height: 1.1;
            margin: 0 0 1.35rem 0;
        }}
        .filter-note {{
            color: {MUTED};
            font-size: 0.9rem;
            margin-top: 0.9rem;
        }}
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div {{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
        }}
        .stDataFrame {{
            border-radius: 18px;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.08);
        }}
        @media (max-width: 900px) {{
            .block-container {{
                padding-top: 6.25rem;
                padding-left: 0.8rem;
                padding-right: 0.8rem;
            }}
            .pv-kicker {{
                font-size: 0.82rem;
                letter-spacing: 0.12em;
            }}
            .pv-subtitle {{
                font-size: 0.82rem;
                line-height: 1.4;
                margin-bottom: 1rem;
            }}
            .front-title {{
                font-size: 2.4rem;
            }}
            .front-subtitle {{
                font-size: 1.1rem;
                margin-bottom: 1rem;
            }}
            h1 {{
                font-size: 2rem !important;
                line-height: 1.05 !important;
            }}
            .filter-note {{
                font-size: 0.82rem;
            }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def position_group(position: str) -> str:
    position = (position or "").upper()
    if "GK" in position or "GOAL" in position:
        return "GK"
    if any(token in position for token in ["CB", "BACK", "DEF", "DC", "DL", "DR", "DM"]):
        return "DEF"
    if any(token in position for token in ["FW", "ST", "RW", "LW", "FORW", "WING"]):
        return "FWD"
    return "MID"

CUSTOM_POSITION_ORDER = [
    "Center Back",
    "Outside Back",
    "Outside Midfielder",
    "Winger",
    "Central Defensive Midfielder",
    "Central Midfielder",
    "Central Attacking Midfielder",
    "Striker",
]

def map_custom_position_from_profile(
    avg_x: float,
    avg_y: float,
    raw_position: str,
    pv_defending: float,
    pv_shooting: float,
    pv_passing: float,
    pv_receiving: float,
    pv_carrying: float,
) -> str:
    """
    Assign custom role primarily from average event coordinates.
    Coordinate assumptions:
      - x increases from own goal to opponent goal
      - y near middle is central; low/high are wide channels
    """
    x = pd.to_numeric(avg_x, errors="coerce")
    y = pd.to_numeric(avg_y, errors="coerce")

    if pd.isna(x) or pd.isna(y):
        # Coordinate fallback if data is missing
        pos = (raw_position or "").upper()
        if any(t in pos for t in ["DC", "CB"]):
            return "Center Back"
        if any(t in pos for t in ["DL", "DR", "LB", "RB", "LWB", "RWB"]):
            return "Outside Back"
        if any(t in pos for t in ["DMC", "CDM"]):
            return "Central Defensive Midfielder"
        if any(t in pos for t in ["AMC", "CAM"]):
            return "Central Attacking Midfielder"
        if any(t in pos for t in ["FW", "ST", "CF"]):
            return "Striker"
        if any(t in pos for t in ["AMR", "AML", "RW", "LW", "WING"]):
            return "Winger"
        if any(t in pos for t in ["ML", "MR"]):
            return "Outside Midfielder"
        return "Central Midfielder"

    # Wider central lane to avoid true CBs getting classified as outside backs
    # and to reduce over-classification of wide roles.
    central = 30 <= y <= 70
    wide = not central

    pos_upper = (raw_position or "").upper()

    # Keep only strongest canonical anchors; otherwise classify from profile + location.
    if any(t in pos_upper for t in ["DC", "CB"]):
        return "Center Back"
    if any(t in pos_upper for t in ["FW", "ST", "CF"]):
        return "Striker" if central else "Winger"

    # Profile-based striker detection for dropping forwards:
    # high shooting contribution and advanced average x.
    shoot = float(pd.to_numeric(pv_shooting, errors="coerce") or 0.0)
    defend = float(pd.to_numeric(pv_defending, errors="coerce") or 0.0)
    passv = float(pd.to_numeric(pv_passing, errors="coerce") or 0.0)
    recv = float(pd.to_numeric(pv_receiving, errors="coerce") or 0.0)
    carry = float(pd.to_numeric(pv_carrying, errors="coerce") or 0.0)
    att_total = max(1e-9, shoot + passv + recv + carry)
    total_with_def = max(1e-9, shoot + passv + recv + carry + max(defend, 0.0))
    shooting_share = shoot / att_total
    defending_share = max(defend, 0.0) / total_with_def

    # Any player with strong shot-share and reasonably advanced territory is a striker.
    if (x >= 56 and shooting_share >= 0.30) or (x >= 50 and shoot >= 1.0):
        return "Striker" if central else "Winger"

    # Wide-lane role assignment is primarily depth + defending profile.
    # Deeper and defense-heavy wide players -> Outside Back.
    if wide:
        if x <= 52 and defending_share >= 0.12:
            return "Outside Back"
        if x <= 60 and defending_share >= 0.16:
            return "Outside Back"
        # Mid-height wide roles -> Outside Midfielder
        if x <= 66:
            return "Outside Midfielder"
        # High wide roles -> Winger
        return "Winger"

    # Central lanes by depth
    if x < 40:
        return "Center Back" if central else "Outside Back"

    # Deep-to-middle band
    if x < 54:
        return "Central Defensive Midfielder" if central else "Outside Midfielder"

    # Advanced midfield band
    if x < 64:
        return "Central Attacking Midfielder" if central else "Winger"

    # Final third
    return "Striker" if central else "Winger"


@st.cache_data(ttl=900, show_spinner=False)
def load_team_data() -> pd.DataFrame:
    query = """
    WITH matches_2026 AS (
        SELECT match_id
        FROM public.matches
        WHERE match_date >= DATE '2026-01-01'
          AND match_date < DATE '2027-01-01'
    ),
    match_events_2026 AS (
        SELECT
            e.match_id,
            e.team_id,
            COALESCE(e.gplus, 0) AS gplus,
            COALESCE(e.gplus_passing, 0) AS gplus_passing,
            COALESCE(e.gplus_receiving, 0) AS gplus_receiving,
            COALESCE(e.gplus_carrying, 0) AS gplus_carrying,
            COALESCE(e.gplus_shooting, 0) AS gplus_shooting,
            COALESCE(e.gplus_defending, 0) AS gplus_defending
        FROM public.match_event e
        INNER JOIN matches_2026 m
            ON m.match_id = e.match_id
        WHERE e.team_id IS NOT NULL
    ),
    team_lookup AS (
        SELECT DISTINCT ON (regexp_replace(team_id, '_\\(\\d{4}\\)$', ''))
            regexp_replace(team_id, '_\\(\\d{4}\\)$', '') AS team_id_raw,
            name AS team_name
        FROM public.teams
        ORDER BY regexp_replace(team_id, '_\\(\\d{4}\\)$', ''), (team_id LIKE '%(2026)') DESC, team_id
    )
    SELECT
        COALESCE(t.team_name, 'Unknown Team') AS team_name,
        COUNT(DISTINCT e.match_id) AS matches,
        SUM(e.gplus) AS pv_total,
        SUM(e.gplus_passing) AS pv_passing,
        SUM(e.gplus_receiving) AS pv_receiving,
        SUM(e.gplus_carrying) AS pv_carrying,
        SUM(e.gplus_shooting) AS pv_shooting,
        SUM(e.gplus_defending) AS pv_defending
    FROM match_events_2026 e
    LEFT JOIN team_lookup t
        ON t.team_id_raw = e.team_id::text
    GROUP BY COALESCE(t.team_name, 'Unknown Team')
    ORDER BY pv_total DESC;
    """

    cfg = db_config()
    conn = psycopg2.connect(
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
    )
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    numeric_cols = [
        "matches",
        "pv_total",
        "pv_passing",
        "pv_receiving",
        "pv_carrying",
        "pv_shooting",
        "pv_defending",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df.sort_values("pv_total", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=900, show_spinner=False)
def load_team_against_data() -> pd.DataFrame:
    query = """
    WITH matches_2026 AS (
        SELECT match_id, home_team_name, away_team_name
        FROM public.matches
        WHERE match_date >= DATE '2026-01-01'
          AND match_date < DATE '2027-01-01'
    ),
    match_events_2026 AS (
        SELECT
            e.match_id,
            e.team_id,
            COALESCE(e.gplus, 0) AS gplus,
            COALESCE(e.gplus_passing, 0) AS gplus_passing,
            COALESCE(e.gplus_receiving, 0) AS gplus_receiving,
            COALESCE(e.gplus_carrying, 0) AS gplus_carrying,
            COALESCE(e.gplus_shooting, 0) AS gplus_shooting,
            COALESCE(e.gplus_defending, 0) AS gplus_defending
        FROM public.match_event e
        INNER JOIN matches_2026 m
            ON m.match_id = e.match_id
        WHERE e.team_id IS NOT NULL
    ),
    team_lookup AS (
        SELECT DISTINCT ON (regexp_replace(team_id, '_\\(\\d{4}\\)$', ''))
            regexp_replace(team_id, '_\\(\\d{4}\\)$', '') AS team_id_raw,
            name AS team_name
        FROM public.teams
        ORDER BY regexp_replace(team_id, '_\\(\\d{4}\\)$', ''), (team_id LIKE '%(2026)') DESC, team_id
    ),
    events_with_team AS (
        SELECT
            e.match_id,
            COALESCE(t.team_name, 'Unknown Team') AS event_team_name,
            e.gplus,
            e.gplus_passing,
            e.gplus_receiving,
            e.gplus_carrying,
            e.gplus_shooting,
            e.gplus_defending
        FROM match_events_2026 e
        LEFT JOIN team_lookup t
            ON t.team_id_raw = e.team_id::text
    ),
    events_with_opponent AS (
        SELECT
            e.match_id,
            CASE
                WHEN e.event_team_name = m.home_team_name THEN m.away_team_name
                WHEN e.event_team_name = m.away_team_name THEN m.home_team_name
                ELSE NULL
            END AS against_team_name,
            e.gplus,
            e.gplus_passing,
            e.gplus_receiving,
            e.gplus_carrying,
            e.gplus_shooting,
            e.gplus_defending
        FROM events_with_team e
        INNER JOIN matches_2026 m
            ON m.match_id = e.match_id
    )
    SELECT
        against_team_name AS team_name,
        COUNT(DISTINCT match_id) AS matches,
        SUM(gplus) AS pv_total,
        SUM(gplus_passing) AS pv_passing,
        SUM(gplus_receiving) AS pv_receiving,
        SUM(gplus_carrying) AS pv_carrying,
        SUM(gplus_shooting) AS pv_shooting,
        SUM(gplus_defending) AS pv_defending
    FROM events_with_opponent
    WHERE against_team_name IS NOT NULL
    GROUP BY against_team_name
    ORDER BY pv_total DESC;
    """

    cfg = db_config()
    conn = psycopg2.connect(
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
    )
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    numeric_cols = [
        "matches",
        "pv_total",
        "pv_passing",
        "pv_receiving",
        "pv_carrying",
        "pv_shooting",
        "pv_defending",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df.sort_values("pv_total", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=900, show_spinner=False)
def load_player_data() -> pd.DataFrame:
    query = """
    WITH matches_2026 AS (
        SELECT match_id
        FROM public.matches
        WHERE match_date >= DATE '2026-01-01'
          AND match_date < DATE '2027-01-01'
    ),
    match_events_2026 AS (
        SELECT
            e.match_id,
            e.player_id,
            e.team_id,
            e.total_mins,
            COALESCE(e.x, 50) AS x,
            COALESCE(e.y, 50) AS y,
            COALESCE(e.gplus, 0) AS gplus,
            COALESCE(e.gplus_passing, 0) AS gplus_passing,
            COALESCE(e.gplus_receiving, 0) AS gplus_receiving,
            COALESCE(e.gplus_carrying, 0) AS gplus_carrying,
            COALESCE(e.gplus_shooting, 0) AS gplus_shooting,
            COALESCE(e.gplus_defending, 0) AS gplus_defending
        FROM public.match_event e
        INNER JOIN matches_2026 m
            ON m.match_id = e.match_id
        WHERE e.player_id IS NOT NULL
    ),
    player_lookup AS (
        SELECT DISTINCT ON (regexp_replace(player_id, '_\\(\\d{4}\\)$', ''))
            regexp_replace(player_id, '_\\(\\d{4}\\)$', '') AS player_id_raw,
            name AS player_name,
            age AS player_age,
            position
        FROM public.players
        ORDER BY regexp_replace(player_id, '_\\(\\d{4}\\)$', ''), (player_id LIKE '%(2026)') DESC, player_id
    ),
    team_lookup AS (
        SELECT DISTINCT ON (regexp_replace(team_id, '_\\(\\d{4}\\)$', ''))
            regexp_replace(team_id, '_\\(\\d{4}\\)$', '') AS team_id_raw,
            name AS team_name
        FROM public.teams
        ORDER BY regexp_replace(team_id, '_\\(\\d{4}\\)$', ''), (team_id LIKE '%(2026)') DESC, team_id
    ),
    player_match_minutes AS (
        SELECT
            player_id,
            match_id,
            MAX(COALESCE(total_mins, 0)) AS match_minutes
        FROM match_events_2026
        GROUP BY player_id, match_id
    ),
    minutes_agg AS (
        SELECT
            player_id,
            COUNT(DISTINCT match_id) AS matches,
            COALESCE(SUM(match_minutes), 0) AS minutes_played
        FROM player_match_minutes
        GROUP BY player_id
    ),
    event_agg AS (
        SELECT
            player_id,
            team_id,
            COUNT(*) AS actions,
            SUM(gplus) AS pv_total,
            SUM(gplus_passing) AS pv_passing,
            SUM(gplus_receiving) AS pv_receiving,
            SUM(gplus_carrying) AS pv_carrying,
            SUM(gplus_shooting) AS pv_shooting,
            SUM(gplus_defending) AS pv_defending,
            AVG(x) AS avg_x,
            AVG(y) AS avg_y
        FROM match_events_2026
        GROUP BY player_id, team_id
    )
    SELECT
        e.player_id,
        COALESCE(p.player_name, 'Unknown Player') AS player_name,
        p.player_age,
        COALESCE(t.team_name, 'Unknown Team') AS team_name,
        COALESCE(p.position, '') AS position,
        COALESCE(m.matches, 0) AS matches,
        COALESCE(m.minutes_played, 0) AS minutes_played,
        COALESCE(e.actions, 0) AS actions,
        e.pv_total,
        e.pv_passing,
        e.pv_receiving,
        e.pv_carrying,
        e.pv_shooting,
        e.pv_defending,
        e.avg_x,
        e.avg_y
    FROM event_agg e
    LEFT JOIN player_lookup p
        ON p.player_id_raw = e.player_id::text
    LEFT JOIN team_lookup t
        ON t.team_id_raw = e.team_id::text
    LEFT JOIN minutes_agg m
        ON m.player_id = e.player_id
    ORDER BY e.pv_total DESC;
    """

    cfg = db_config()
    conn = psycopg2.connect(
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
    )
    try:
        df = pd.read_sql(query, conn)

        # Build defender-context defensive score inputs at player-match granularity.
        defender_context_query = """
        WITH matches_2026 AS (
            SELECT match_id, home_team_name, away_team_name
            FROM public.matches
            WHERE match_date >= DATE '2026-01-01'
              AND match_date < DATE '2027-01-01'
        ),
        match_events_2026 AS (
            SELECT
                e.match_id,
                e.player_id,
                e.team_id,
                e.total_mins,
                COALESCE(e.gplus_passing, 0) AS gplus_passing,
                COALESCE(e.gplus_receiving, 0) AS gplus_receiving,
                COALESCE(e.gplus_carrying, 0) AS gplus_carrying,
                COALESCE(e.gplus_shooting, 0) AS gplus_shooting,
                COALESCE(e.gplus_defending, 0) AS gplus_defending
            FROM public.match_event e
            INNER JOIN matches_2026 m
                ON m.match_id = e.match_id
            WHERE e.player_id IS NOT NULL
              AND e.team_id IS NOT NULL
        ),
        team_lookup AS (
            SELECT DISTINCT ON (regexp_replace(team_id, '_\\(\\d{4}\\)$', ''))
                regexp_replace(team_id, '_\\(\\d{4}\\)$', '') AS team_id_raw,
                name AS team_name
            FROM public.teams
            ORDER BY regexp_replace(team_id, '_\\(\\d{4}\\)$', ''), (team_id LIKE '%(2026)') DESC, team_id
        ),
        player_match AS (
            SELECT
                e.player_id,
                e.match_id,
                e.team_id,
                MAX(COALESCE(e.total_mins, 0)) AS match_minutes,
                SUM(e.gplus_defending) AS pv_defending_match
            FROM match_events_2026 e
            GROUP BY e.player_id, e.match_id, e.team_id
        ),
        team_match_att_pv AS (
            SELECT
                e.match_id,
                COALESCE(t.team_name, 'Unknown Team') AS team_name,
                SUM(e.gplus_passing + e.gplus_receiving + e.gplus_carrying + e.gplus_shooting) AS team_att_pv_match
            FROM match_events_2026 e
            LEFT JOIN team_lookup t
                ON t.team_id_raw = e.team_id::text
            GROUP BY e.match_id, COALESCE(t.team_name, 'Unknown Team')
        ),
        player_match_with_names AS (
            SELECT
                pm.player_id,
                pm.match_id,
                pm.match_minutes,
                pm.pv_defending_match,
                COALESCE(t.team_name, 'Unknown Team') AS team_name
            FROM player_match pm
            LEFT JOIN team_lookup t
                ON t.team_id_raw = pm.team_id::text
        ),
        player_match_with_opp AS (
            SELECT
                pm.player_id,
                pm.match_id,
                pm.match_minutes,
                pm.pv_defending_match,
                CASE
                    WHEN pm.team_name = m.home_team_name THEN m.away_team_name
                    WHEN pm.team_name = m.away_team_name THEN m.home_team_name
                    ELSE NULL
                END AS opponent_team_name
            FROM player_match_with_names pm
            INNER JOIN matches_2026 m
                ON m.match_id = pm.match_id
        ),
        player_match_context AS (
            SELECT
                pm.player_id,
                pm.match_id,
                pm.match_minutes,
                pm.pv_defending_match,
                COALESCE(ta.team_att_pv_match, 0) AS opponent_att_pv_match
            FROM player_match_with_opp pm
            LEFT JOIN team_match_att_pv ta
                ON ta.match_id = pm.match_id
               AND ta.team_name = pm.opponent_team_name
        )
        SELECT
            player_id,
            SUM(pv_defending_match) AS pv_defending_raw,
            SUM(opponent_att_pv_match * LEAST(GREATEST(match_minutes, 0), 90) / 90.0) AS opponent_att_pv_faced
        FROM player_match_context
        GROUP BY player_id;
        """
        defender_context_df = pd.read_sql(defender_context_query, conn)
    finally:
        conn.close()

    numeric_cols = [
        "player_age",
        "matches",
        "minutes_played",
        "actions",
        "pv_total",
        "pv_passing",
        "pv_receiving",
        "pv_carrying",
        "pv_shooting",
        "pv_defending",
        "avg_x",
        "avg_y",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if len(defender_context_df) > 0:
        defender_context_df["player_id"] = defender_context_df["player_id"].astype(str)
        for col in ["pv_defending_raw", "opponent_att_pv_faced"]:
            defender_context_df[col] = pd.to_numeric(defender_context_df[col], errors="coerce").fillna(0.0)

        df["player_id"] = df["player_id"].astype(str)
        df = df.merge(defender_context_df, on="player_id", how="left")
    else:
        df["pv_defending_raw"] = df["pv_defending"]
        df["opponent_att_pv_faced"] = 0.0

    df["pv_defending_raw"] = pd.to_numeric(df["pv_defending_raw"], errors="coerce").fillna(df["pv_defending"])
    df["opponent_att_pv_faced"] = pd.to_numeric(df["opponent_att_pv_faced"], errors="coerce").fillna(0.0)

    eps = 0.05
    df["def_adj_ratio"] = df["pv_defending_raw"] / df["opponent_att_pv_faced"].clip(lower=eps)

    df["pv_per_90"] = np.where(
        df["minutes_played"] > 0,
        (df["pv_total"] / df["minutes_played"]) * 90.0,
        0.0,
    )
    df["pv_per_action"] = np.where(
        df["actions"] > 0,
        df["pv_total"] / df["actions"],
        0.0,
    )
    # Volume-aware performer metric:
    # - Shrinks noisy small samples toward league rate
    # - Rewards sustained outperformance over larger action totals
    prior_actions = 300.0
    total_actions = float(df["actions"].sum())
    league_rate = float(df["pv_total"].sum()) / total_actions if total_actions > 0 else 0.0
    df["stabilized_pv_per_action"] = np.where(
        (df["actions"] + prior_actions) > 0,
        (df["pv_total"] + prior_actions * league_rate) / (df["actions"] + prior_actions),
        league_rate,
    )
    df["performance_score"] = (df["stabilized_pv_per_action"] - league_rate) * df["actions"]

    df["position_group"] = df["position"].apply(position_group)
    df["position_custom"] = df.apply(
        lambda r: map_custom_position_from_profile(
            r.get("avg_x"),
            r.get("avg_y"),
            r.get("position"),
            r.get("pv_defending"),
            r.get("pv_shooting"),
            r.get("pv_passing"),
            r.get("pv_receiving"),
            r.get("pv_carrying"),
        ),
        axis=1,
    )

    # Replace displayed defensive score with defender-context standardized value for defenders.
    # (Non-defenders retain their raw defensive PV+ value.)
    defenders_mask = df["position_group"] == "DEF"
    if defenders_mask.any():
        def_ratio = pd.to_numeric(df.loc[defenders_mask, "def_adj_ratio"], errors="coerce").fillna(0.0)
        ratio_std = def_ratio.std(ddof=0)
        if ratio_std > 0 and not np.isnan(ratio_std):
            z_def = (def_ratio - def_ratio.mean()) / ratio_std
        else:
            z_def = pd.Series(np.zeros(len(def_ratio)), index=def_ratio.index)
        df.loc[defenders_mask, "pv_defending"] = z_def

    return df


@st.cache_data(show_spinner=False)
def team_logo_path(team_name: str) -> str | None:
    logo_id = TEAM_LOGO_IDS.get(team_name)
    if not logo_id:
        return None
    logo_path = Path(__file__).resolve().parent / "assets" / "mls_logos" / f"{logo_id}_image.png"
    if not logo_path.exists():
        return None
    return str(logo_path)


def add_team_logo_column(df: pd.DataFrame, team_col: str = "team_name") -> pd.DataFrame:
    enriched_df = df.copy()
    enriched_df["team_logo"] = enriched_df[team_col].apply(team_logo_path)
    return enriched_df


def style_rankings_table(df: pd.DataFrame, numeric_columns: list[str]) -> pd.io.formats.style.Styler:
    def highlight_top_10(series: pd.Series) -> list[str]:
        numeric = pd.to_numeric(series, errors="coerce")
        threshold = numeric.quantile(0.9)
        styles = []
        for value in numeric:
            if pd.notna(value) and value >= threshold:
                styles.append("background-color: rgba(127, 200, 255, 0.22);")
            else:
                styles.append("")
        return styles

    return (
        df.style.apply(highlight_top_10, subset=numeric_columns, axis=0)
        .format(
            {
                "Age": "{:.0f}",
                "Matches": "{:.0f}",
                "Actions": "{:.0f}",
                "Minutes": "{:.0f}",
                "PV+ Per 90": "{:.2f}",
                "PV+": "{:.2f}",
                "Total PV+": "{:.2f}",
                "Passing": "{:.2f}",
                "Receiving": "{:.2f}",
                "Carrying": "{:.2f}",
                "Shooting": "{:.2f}",
                "Defending": "{:.2f}",
                "PV+ Against": "{:.2f}",
                "Passing Against": "{:.2f}",
                "Receiving Against": "{:.2f}",
                "Carrying Against": "{:.2f}",
                "Shooting Against": "{:.2f}",
                "Defending Against": "{:.2f}",
            },
            na_rep="",
        )
    )


inject_styles()

st.markdown(
    '<div class="front-title">Sunday League Stats</div>'
    '<div class="front-subtitle">Possession Value Added</div>',
    unsafe_allow_html=True,
)

try:
    df = load_player_data()
    team_df = load_team_data()
    team_against_df = load_team_against_data()
except Exception as exc:
    st.error(f"Could not load data from Supabase: {exc}")
    st.stop()
df = df[df["position_group"] != "GK"].copy()
df = add_team_logo_column(df)
team_df = add_team_logo_column(team_df)
team_against_df = add_team_logo_column(team_against_df)
player_tab, team_tab = st.tabs(["Player Rankings", "Team Rankings"])

with player_tab:
    team_options = ["All Teams"] + sorted(df["team_name"].dropna().unique().tolist())
    position_options = ["All Positions"] + CUSTOM_POSITION_ORDER
    default_max_age = 40

    with st.container():
        filter_cols = st.columns([1.0, 1.0, 0.8, 0.9, 0.85])

        with filter_cols[0]:
            team_filter = st.selectbox("Team", options=team_options, key="player_team_filter")
        with filter_cols[1]:
            player_filter = st.text_input("Player name", placeholder="Search player...", key="player_name_filter")
        with filter_cols[2]:
            max_age = st.number_input("Max age", min_value=0, value=default_max_age, step=1, key="player_age_filter")
        with filter_cols[3]:
            position_filter = st.selectbox("Position", options=position_options, key="player_position_filter")
        with filter_cols[4]:
            min_minutes = st.number_input(
                "Minimum minutes",
                min_value=0,
                value=0,
                step=90,
                key="player_minutes_filter",
            )

    filtered_df = df.copy()
    if team_filter != "All Teams":
        filtered_df = filtered_df[filtered_df["team_name"] == team_filter]
    if player_filter:
        filtered_df = filtered_df[
            filtered_df["player_name"].str.contains(player_filter, case=False, na=False)
        ]
    filtered_df = filtered_df[
        filtered_df["player_age"].fillna(999) <= max_age
    ]
    if position_filter != "All Positions":
        filtered_df = filtered_df[filtered_df["position_custom"] == position_filter]
    filtered_df = filtered_df[filtered_df["minutes_played"] >= min_minutes]
    filtered_df = filtered_df.sort_values("pv_per_90", ascending=False).reset_index(drop=True)
    filtered_df["rank"] = range(1, len(filtered_df) + 1)

    st.markdown(
        """
        <div class="filter-note">
            All players are included by default. Top 10% performers for each PV+ category are highlighted blue.
        </div>
        """,
        unsafe_allow_html=True,
    )

    display_df = filtered_df[
        [
            "rank",
            "player_name",
            "player_age",
            "team_name",
            "position_custom",
            "matches",
            "minutes_played",
            "actions",
            "pv_per_90",
            "pv_total",
            "pv_passing",
            "pv_receiving",
            "pv_carrying",
            "pv_shooting",
            "pv_defending",
        ]
    ].rename(
        columns={
            "rank": "Rank",
            "player_name": "Player",
            "player_age": "Age",
            "team_name": "Team",
            "position_custom": "Position",
            "matches": "Matches",
            "minutes_played": "Minutes",
            "actions": "Actions",
            "pv_per_90": "PV+ Per 90",
            "pv_total": "Total PV+",
            "pv_passing": "Passing",
            "pv_receiving": "Receiving",
            "pv_carrying": "Carrying",
            "pv_shooting": "Shooting",
            "pv_defending": "Defending",
        }
    )

    st.dataframe(
        style_rankings_table(
            display_df,
            numeric_columns=["PV+ Per 90", "Total PV+", "Passing", "Receiving", "Carrying", "Shooting", "Defending"],
        ),
        use_container_width=True,
        hide_index=True,
        height=780,
        column_config={
            "Rank": st.column_config.NumberColumn("Rank", format="%d"),
            "Team": st.column_config.TextColumn("Team", width="medium"),
            "Age": st.column_config.NumberColumn("Age", format="%d"),
            "Matches": st.column_config.NumberColumn("Matches", format="%d"),
            "Minutes": st.column_config.NumberColumn("Minutes", format="%d"),
            "Actions": st.column_config.NumberColumn("Actions", format="%d"),
            "PV+ Per 90": st.column_config.NumberColumn("PV+ Per 90", format="%.2f"),
            "Total PV+": st.column_config.NumberColumn("Total PV+", format="%.2f"),
            "Passing": st.column_config.NumberColumn("Passing", format="%.2f"),
            "Receiving": st.column_config.NumberColumn("Receiving", format="%.2f"),
            "Carrying": st.column_config.NumberColumn("Carrying", format="%.2f"),
            "Shooting": st.column_config.NumberColumn("Shooting", format="%.2f"),
            "Defending": st.column_config.NumberColumn("Defending", format="%.2f"),
        },
    )

with team_tab:
        teams_for_tab, teams_against_tab = st.tabs(["For", "Against"])

        with teams_for_tab:
            filtered_team_df = team_df.copy().sort_values("pv_total", ascending=False).reset_index(drop=True)
            filtered_team_df["rank"] = range(1, len(filtered_team_df) + 1)

            st.markdown(
                f"""
                <div class="filter-note">
                    Showing <strong>{len(filtered_team_df):,}</strong> teams from the 2026 season.
                </div>
                """,
                unsafe_allow_html=True,
            )

            team_display_df = filtered_team_df[
                [
                    "rank",
                    "team_name",
                    "matches",
                    "pv_total",
                    "pv_passing",
                    "pv_receiving",
                    "pv_carrying",
                    "pv_shooting",
                    "pv_defending",
                ]
            ].rename(
                columns={
                    "rank": "Rank",
                    "team_name": "Team",
                    "matches": "Matches",
                    "pv_total": "PV+",
                    "pv_passing": "Passing",
                    "pv_receiving": "Receiving",
                    "pv_carrying": "Carrying",
                    "pv_shooting": "Shooting",
                    "pv_defending": "Defending",
                }
            )

            st.dataframe(
                style_rankings_table(
                    team_display_df,
                    numeric_columns=["PV+", "Passing", "Receiving", "Carrying", "Shooting", "Defending"],
                ),
                use_container_width=True,
                hide_index=True,
                height=780,
                column_config={
                    "Rank": st.column_config.NumberColumn("Rank", format="%d"),
                    "Team": st.column_config.TextColumn("Team", width="medium"),
                    "Matches": st.column_config.NumberColumn("Matches", format="%d"),
                    "PV+": st.column_config.NumberColumn("PV+", format="%.2f"),
                    "Passing": st.column_config.NumberColumn("Passing", format="%.2f"),
                    "Receiving": st.column_config.NumberColumn("Receiving", format="%.2f"),
                    "Carrying": st.column_config.NumberColumn("Carrying", format="%.2f"),
                    "Shooting": st.column_config.NumberColumn("Shooting", format="%.2f"),
                    "Defending": st.column_config.NumberColumn("Defending", format="%.2f"),
                },
            )

        with teams_against_tab:
            against_df = team_against_df.copy().sort_values("pv_total", ascending=False).reset_index(drop=True)
            against_df["rank"] = range(1, len(against_df) + 1)

            st.markdown(
                f"""
                <div class="filter-note">
                    Showing PV+ earned <strong>against</strong> each team in the 2026 season.
                </div>
                """,
                unsafe_allow_html=True,
            )

            against_display_df = against_df[
                [
                    "rank",
                    "team_name",
                    "matches",
                    "pv_total",
                    "pv_passing",
                    "pv_receiving",
                    "pv_carrying",
                    "pv_shooting",
                    "pv_defending",
                ]
            ].rename(
                columns={
                    "rank": "Rank",
                    "team_name": "Team",
                    "matches": "Matches",
                    "pv_total": "PV+ Against",
                    "pv_passing": "Passing Against",
                    "pv_receiving": "Receiving Against",
                    "pv_carrying": "Carrying Against",
                    "pv_shooting": "Shooting Against",
                    "pv_defending": "Defending Against",
                }
            )

            st.dataframe(
                style_rankings_table(
                    against_display_df,
                    numeric_columns=[
                        "PV+ Against",
                        "Passing Against",
                        "Receiving Against",
                        "Carrying Against",
                        "Shooting Against",
                        "Defending Against",
                    ],
                ),
                use_container_width=True,
                hide_index=True,
                height=780,
                column_config={
                    "Rank": st.column_config.NumberColumn("Rank", format="%d"),
                    "Team": st.column_config.TextColumn("Team", width="medium"),
                    "Matches": st.column_config.NumberColumn("Matches", format="%d"),
                    "PV+ Against": st.column_config.NumberColumn("PV+ Against", format="%.2f"),
                    "Passing Against": st.column_config.NumberColumn("Passing Against", format="%.2f"),
                    "Receiving Against": st.column_config.NumberColumn("Receiving Against", format="%.2f"),
                    "Carrying Against": st.column_config.NumberColumn("Carrying Against", format="%.2f"),
                    "Shooting Against": st.column_config.NumberColumn("Shooting Against", format="%.2f"),
                    "Defending Against": st.column_config.NumberColumn("Defending Against", format="%.2f"),
                },
            )
