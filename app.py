from __future__ import annotations

import os
import base64
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
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
        .header-crest {{
            display: flex;
            justify-content: flex-end;
            align-items: flex-start;
            width: 100%;
            padding-top: 0.1rem;
            margin-right: -0.2rem;
        }}
        .header-crest img {{
            display: block;
            margin-left: auto;
            filter: drop-shadow(0 8px 18px rgba(0,0,0,0.22));
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
            .header-crest {{
                padding-top: 0.05rem;
                margin-right: 0;
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
            SUM(gplus_defending) AS pv_defending
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
        e.pv_total,
        e.pv_passing,
        e.pv_receiving,
        e.pv_carrying,
        e.pv_shooting,
        e.pv_defending
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
    finally:
        conn.close()

    numeric_cols = [
        "player_age",
        "matches",
        "minutes_played",
        "pv_total",
        "pv_passing",
        "pv_receiving",
        "pv_carrying",
        "pv_shooting",
        "pv_defending",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["position_group"] = df["position"].apply(position_group)
    return df


@st.cache_data(ttl=900, show_spinner=False)
def load_player_match_trends() -> pd.DataFrame:
    query = """
    WITH matches_2026 AS (
        SELECT match_id, match_date, home_team_name, away_team_name
        FROM public.matches
        WHERE match_date >= DATE '2026-01-01'
          AND match_date < DATE '2027-01-01'
    ),
    match_events_2026 AS (
        SELECT
            e.match_id,
            e.player_id,
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
        WHERE e.player_id IS NOT NULL
    ),
    player_lookup AS (
        SELECT DISTINCT ON (regexp_replace(player_id, '_\\(\\d{4}\\)$', ''))
            regexp_replace(player_id, '_\\(\\d{4}\\)$', '') AS player_id_raw,
            name AS player_name
        FROM public.players
        ORDER BY regexp_replace(player_id, '_\\(\\d{4}\\)$', ''), (player_id LIKE '%(2026)') DESC, player_id
    ),
    team_lookup AS (
        SELECT DISTINCT ON (regexp_replace(team_id, '_\\(\\d{4}\\)$', ''))
            regexp_replace(team_id, '_\\(\\d{4}\\)$', '') AS team_id_raw,
            name AS team_name
        FROM public.teams
        ORDER BY regexp_replace(team_id, '_\\(\\d{4}\\)$', ''), (team_id LIKE '%(2026)') DESC, team_id
    )
    SELECT
        e.player_id,
        COALESCE(p.player_name, 'Unknown Player') AS player_name,
        COALESCE(t.team_name, 'Unknown Team') AS team_name,
        m.match_id,
        m.match_date,
        m.home_team_name,
        m.away_team_name,
        SUM(e.gplus) AS pv_total,
        SUM(e.gplus_passing) AS pv_passing,
        SUM(e.gplus_receiving) AS pv_receiving,
        SUM(e.gplus_carrying) AS pv_carrying,
        SUM(e.gplus_shooting) AS pv_shooting,
        SUM(e.gplus_defending) AS pv_defending
    FROM match_events_2026 e
    INNER JOIN matches_2026 m
        ON m.match_id = e.match_id
    LEFT JOIN player_lookup p
        ON p.player_id_raw = e.player_id::text
    LEFT JOIN team_lookup t
        ON t.team_id_raw = e.team_id::text
    GROUP BY
        e.player_id, p.player_name, t.team_name,
        m.match_id, m.match_date, m.home_team_name, m.away_team_name
    ORDER BY m.match_date, m.match_id;
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
        "pv_total",
        "pv_passing",
        "pv_receiving",
        "pv_carrying",
        "pv_shooting",
        "pv_defending",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def chart_layout(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=1, xanchor="right"),
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.08)", zeroline=False, tickfont=dict(color=MUTED))
    return fig


@st.cache_data(show_spinner=False)
def team_logo_data_uri(team_name: str) -> str | None:
    logo_id = TEAM_LOGO_IDS.get(team_name)
    if not logo_id:
        return None
    logo_path = Path(__file__).resolve().parent / "assets" / "mls_logos" / f"{logo_id}_image.png"
    if not logo_path.exists():
        return None
    encoded = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def add_team_logo_column(df: pd.DataFrame, team_col: str = "team_name") -> pd.DataFrame:
    enriched_df = df.copy()
    enriched_df["team_logo"] = enriched_df[team_col].apply(team_logo_data_uri)
    return enriched_df


def set_selected_player(player_id: str | None) -> None:
    if player_id:
        st.query_params["player_id"] = str(player_id)
    else:
        st.query_params.clear()


def get_selected_player_id() -> str | None:
    player_id = st.query_params.get("player_id")
    if player_id is None:
        return None
    if isinstance(player_id, list):
        return player_id[0] if player_id else None
    return str(player_id)


def player_pizza_chart(selected_player_row: pd.Series, population_df: pd.DataFrame):
    categories = [
        ("Passing", "pv_passing"),
        ("Receiving", "pv_receiving"),
        ("Carrying", "pv_carrying"),
        ("Shooting", "pv_shooting"),
        ("Defending", "pv_defending"),
    ]

    labels = []
    values = []
    palette = ["#86DF81", "#79CD74", "#73C16E", "#80CE79", "#9AF090"]

    for label, col in categories:
        series = pd.to_numeric(population_df[col], errors="coerce").fillna(0)
        percentile = float((series <= float(selected_player_row[col])).mean() * 100)
        labels.append(label)
        values.append(max(percentile, 1))

    values = np.array(values)
    count = len(labels)
    angles = np.linspace(0, 2 * np.pi, count, endpoint=False)
    width = (2 * np.pi / count) * 0.995

    fig = plt.figure(figsize=(7.2, 7.8), facecolor=PANEL)
    ax = plt.subplot(111, polar=True, facecolor=PANEL)
    fig.subplots_adjust(top=0.80, left=0.11, right=0.89, bottom=0.08)
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_ylim(0, 100)

    bars = ax.bar(
        angles,
        values,
        width=width,
        bottom=0,
        color=palette,
        edgecolor="#ECEFF4",
        linewidth=1.8,
        alpha=0.96,
        align="center",
        zorder=3,
    )

    for bar in bars:
        bar.set_joinstyle("miter")

    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels([])
    ax.yaxis.grid(True, color=(1, 1, 1, 0.38), linestyle="--", linewidth=1.0)
    ax.xaxis.grid(True, color=(1, 1, 1, 0.12), linestyle="-", linewidth=0.9)
    ax.spines["polar"].set_visible(False)

    ax.set_xticks(angles)
    ax.set_xticklabels(labels, color=TEXT, fontsize=15)
    ax.tick_params(axis="x", pad=12)

    centre = plt.Circle((0, 0), 12, transform=ax.transData._b, color=BG, zorder=5)
    ax.add_artist(centre)
    outline = plt.Circle((0, 0), 12, transform=ax.transData._b, fill=False, color="#ECEFF4", linewidth=1.5, zorder=6)
    ax.add_artist(outline)

    for angle, value in zip(angles, values):
        ax.text(
            angle,
            min(value + 7, 102),
            f"{int(round(value))}",
            ha="center",
            va="center",
            color=TEXT,
            fontsize=13,
            fontweight="bold",
            bbox=dict(
                boxstyle="round,pad=0.35",
                facecolor="#181C24",
                edgecolor="#ECEFF4",
                linewidth=1.5,
            ),
            zorder=7,
        )

    fig.text(
        0.08,
        0.95,
        selected_player_row["player_name"],
        color=TEXT,
        fontsize=24,
        fontweight="bold",
        ha="left",
    )
    fig.text(
        0.08,
        0.905,
        f"{selected_player_row['team_name']} | {selected_player_row['position_group']} | MLS 2026",
        color=TEXT,
        fontsize=15,
        fontweight="bold",
        ha="left",
    )

    return fig


def player_trend_chart(trend_df: pd.DataFrame, player_name: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=trend_df["match_date"],
            y=trend_df["pv_total"],
            mode="lines+markers",
            line=dict(color=GOLD, width=4),
            marker=dict(size=10, color=TEXT, line=dict(color=GOLD, width=2)),
            hovertext=trend_df["match_label"],
            hovertemplate="%{hovertext}<br>PV+: %{y:.2f}<extra></extra>",
            name="PV+",
        )
    )
    fig.update_layout(title=f"{player_name} | PV+ Over the Season")
    return chart_layout(fig)


def render_player_detail_screen(
    selected_player_row: pd.Series,
    population_df: pd.DataFrame,
    player_match_df: pd.DataFrame,
) -> None:
    top_cols = st.columns([0.16, 0.84], vertical_alignment="center")
    with top_cols[0]:
        if st.button("Back to rankings", use_container_width=True):
            set_selected_player(None)
            st.rerun()
    with top_cols[1]:
        st.markdown(
            f"""
            <div class="pv-kicker">Player Detail</div>
            <div style="font-size:2.35rem;font-weight:900;color:{TEXT};line-height:1.05;">
                {selected_player_row['player_name']}
            </div>
            <div class="pv-subtitle" style="margin-top:0.45rem;">
                {selected_player_row['team_name']} | {selected_player_row['position_group']} | Age {int(selected_player_row['player_age'])}
            </div>
            """,
            unsafe_allow_html=True,
        )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Rank", f"#{int(selected_player_row['rank'])}")
    metric_cols[1].metric("Matches", f"{int(selected_player_row['matches'])}")
    metric_cols[2].metric("Minutes", f"{int(selected_player_row['minutes_played'])}")
    metric_cols[3].metric("Total PV+", f"{float(selected_player_row['pv_total']):.2f}")

    trend_df = player_match_df[player_match_df["player_id"] == selected_player_row["player_id"]].copy()
    trend_df = trend_df.sort_values(["match_date", "match_id"]).reset_index(drop=True)
    trend_df["match_label"] = trend_df.apply(
        lambda r: f"{r['match_date'].date()} | {r['home_team_name']} vs {r['away_team_name']}",
        axis=1,
    )

    detail_cols = st.columns([1.05, 1.25], vertical_alignment="top")
    with detail_cols[0]:
        st.pyplot(
            player_pizza_chart(selected_player_row, population_df),
            use_container_width=True,
            clear_figure=True,
        )
    with detail_cols[1]:
        st.plotly_chart(
            player_trend_chart(trend_df, selected_player_row["player_name"]),
            use_container_width=True,
        )


inject_styles()

logo_path = Path(__file__).resolve().parent / "assets" / "unnamed.jpg"
title_cols = st.columns([0.93, 0.07], vertical_alignment="top")
with title_cols[0]:
    st.markdown(
        '<div class="front-title">Sunday League Stats</div>'
        '<div class="front-subtitle">Possession Value Added</div>',
        unsafe_allow_html=True,
    )
with title_cols[1]:
    if logo_path.exists():
        st.markdown('<div class="header-crest">', unsafe_allow_html=True)
        st.image(str(logo_path), width=96)
        st.markdown("</div>", unsafe_allow_html=True)

try:
    df = load_player_data()
    team_df = load_team_data()
    team_against_df = load_team_against_data()
    player_match_df = load_player_match_trends()
except Exception as exc:
    st.error(f"Could not load data from Supabase: {exc}")
    st.stop()

selected_player_id = get_selected_player_id()

if selected_player_id:
    ranked_df = df.sort_values("pv_total", ascending=False).reset_index(drop=True).copy()
    ranked_df["rank"] = range(1, len(ranked_df) + 1)
    selected_player_df = ranked_df[ranked_df["player_id"].astype(str) == str(selected_player_id)].copy()

    if selected_player_df.empty:
        st.warning("That player detail view is no longer available. Returning to rankings.")
        set_selected_player(None)
        st.rerun()

    render_player_detail_screen(selected_player_df.iloc[0], df, player_match_df)
else:
    df = add_team_logo_column(df)
    team_df = add_team_logo_column(team_df)
    team_against_df = add_team_logo_column(team_against_df)
    player_tab, team_tab = st.tabs(["Player Rankings", "Team Rankings"])

    with player_tab:
        team_options = ["All Teams"] + sorted(df["team_name"].dropna().unique().tolist())
        position_options = ["All Positions", "GK", "DEF", "MID", "FWD"]

        with st.container():
            filter_cols = st.columns([1.0, 1.0, 0.8, 0.9, 0.75])

            with filter_cols[0]:
                team_filter = st.selectbox("Team", options=team_options, key="player_team_filter")
            with filter_cols[1]:
                player_filter = st.text_input("Player name", placeholder="Search player...", key="player_name_filter")
            with filter_cols[2]:
                max_age = st.number_input("Max age", min_value=0, value=99, step=1, key="player_age_filter")
            with filter_cols[3]:
                position_filter = st.selectbox("Position", options=position_options, key="player_position_filter")
            with filter_cols[4]:
                min_minutes = st.number_input("Minimum minutes", min_value=0, value=0, step=45, key="player_minutes_filter")

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
            filtered_df = filtered_df[filtered_df["position_group"] == position_filter]
        filtered_df = filtered_df[filtered_df["minutes_played"] >= min_minutes]
        filtered_df = filtered_df.sort_values("pv_total", ascending=False).reset_index(drop=True)
        filtered_df["rank"] = range(1, len(filtered_df) + 1)

        st.markdown(
            f"""
            <div class="filter-note">
                Showing <strong>{len(filtered_df):,}</strong> players from the 2026 season. Click a player row to open their detail screen.
            </div>
            """,
            unsafe_allow_html=True,
        )

        display_df = filtered_df[
            [
                "rank",
                "player_name",
                "player_age",
                "team_logo",
                "position_group",
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
                "player_name": "Player",
                "player_age": "Age",
                "team_logo": "Team",
                "position_group": "Position",
                "matches": "Matches",
                "pv_total": "PV+",
                "pv_passing": "Passing",
                "pv_receiving": "Receiving",
                "pv_carrying": "Carrying",
                "pv_shooting": "Shooting",
                "pv_defending": "Defending",
            }
        )

        player_table_event = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=780,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "Rank": st.column_config.NumberColumn("Rank", format="%d"),
                "Team": st.column_config.ImageColumn("Team", help="Club logo", width="small"),
                "Age": st.column_config.NumberColumn("Age", format="%d"),
                "Matches": st.column_config.NumberColumn("Matches", format="%d"),
                "PV+": st.column_config.NumberColumn("PV+", format="%.2f"),
                "Passing": st.column_config.NumberColumn("Passing", format="%.2f"),
                "Receiving": st.column_config.NumberColumn("Receiving", format="%.2f"),
                "Carrying": st.column_config.NumberColumn("Carrying", format="%.2f"),
                "Shooting": st.column_config.NumberColumn("Shooting", format="%.2f"),
                "Defending": st.column_config.NumberColumn("Defending", format="%.2f"),
            },
        )

        selected_rows = player_table_event.selection.rows if player_table_event else []
        if selected_rows:
            selected_row_index = selected_rows[0]
            player_row = filtered_df.iloc[selected_row_index]
            set_selected_player(player_row["player_id"])
            st.rerun()

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
                    "team_logo",
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
                    "team_logo": "Team",
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
                team_display_df,
                use_container_width=True,
                hide_index=True,
                height=780,
                column_config={
                    "Rank": st.column_config.NumberColumn("Rank", format="%d"),
                    "Team": st.column_config.ImageColumn("Team", help="Club logo", width="small"),
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
                    "team_logo",
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
                    "team_logo": "Team",
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
                against_display_df,
                use_container_width=True,
                hide_index=True,
                height=780,
                column_config={
                    "Rank": st.column_config.NumberColumn("Rank", format="%d"),
                    "Team": st.column_config.ImageColumn("Team", help="Club logo", width="small"),
                    "Matches": st.column_config.NumberColumn("Matches", format="%d"),
                    "PV+ Against": st.column_config.NumberColumn("PV+ Against", format="%.2f"),
                    "Passing Against": st.column_config.NumberColumn("Passing Against", format="%.2f"),
                    "Receiving Against": st.column_config.NumberColumn("Receiving Against", format="%.2f"),
                    "Carrying Against": st.column_config.NumberColumn("Carrying Against", format="%.2f"),
                    "Shooting Against": st.column_config.NumberColumn("Shooting Against", format="%.2f"),
                    "Defending Against": st.column_config.NumberColumn("Defending Against", format="%.2f"),
                },
            )
