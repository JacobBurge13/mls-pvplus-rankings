from __future__ import annotations

import os
from dataclasses import dataclass

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
GOLD = "#c7a15a"
BLUE = "#5d86c9"


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
            padding-top: 2rem;
            padding-bottom: 2rem;
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, rgba(19,25,35,0.98), rgba(11,16,24,0.98));
            border-right: 1px solid rgba(255,255,255,0.06);
        }}
        .pv-shell {{
            border: 1px solid rgba(255,255,255,0.08);
            background: linear-gradient(180deg, rgba(19,25,35,0.94), rgba(11,16,24,0.98));
            border-radius: 22px;
            padding: 1.25rem 1.35rem;
            box-shadow: 0 24px 60px rgba(0,0,0,0.28);
        }}
        .pv-kicker {{
            color: {GOLD};
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.78rem;
            font-weight: 800;
        }}
        .pv-title {{
            font-size: 2.6rem;
            line-height: 1;
            font-weight: 900;
            letter-spacing: -0.04em;
            margin: 0.25rem 0 0.5rem 0;
            text-transform: uppercase;
        }}
        .pv-subtitle {{
            color: {MUTED};
            font-size: 0.95rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            font-weight: 700;
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


inject_styles()

st.markdown(
    """
    <div class="pv-shell">
        <div class="pv-kicker">MLS 2026 Season</div>
        <div class="pv-title">PV+ Player Rankings</div>
        <div class="pv-subtitle">Brought to you by Sunday League Stats</div>
    """,
    unsafe_allow_html=True,
)

try:
    df = load_player_data()
except Exception as exc:
    st.error(f"Could not load data from Supabase: {exc}")
    st.stop()

filter_cols = st.columns([1.2, 1.2, 0.8, 0.8])

team_options = ["All Teams"] + sorted(df["team_name"].dropna().unique().tolist())
position_options = ["All Positions", "GK", "DEF", "MID", "FWD"]

with filter_cols[0]:
    team_filter = st.selectbox("Team", options=team_options)
with filter_cols[1]:
    player_filter = st.text_input("Player name", placeholder="Search player...")
with filter_cols[2]:
    position_filter = st.selectbox("Position", options=position_options)
with filter_cols[3]:
    min_minutes = st.number_input("Minimum minutes", min_value=0, value=0, step=45)

filtered_df = df.copy()
if team_filter != "All Teams":
    filtered_df = filtered_df[filtered_df["team_name"] == team_filter]
if player_filter:
    filtered_df = filtered_df[
        filtered_df["player_name"].str.contains(player_filter, case=False, na=False)
    ]
if position_filter != "All Positions":
    filtered_df = filtered_df[filtered_df["position_group"] == position_filter]
filtered_df = filtered_df[filtered_df["minutes_played"] >= min_minutes]
filtered_df = filtered_df.sort_values("pv_total", ascending=False).reset_index(drop=True)

st.markdown(
    f"""
    <div class="filter-note">
        Showing <strong>{len(filtered_df):,}</strong> players from the 2026 season.
    </div>
    """,
    unsafe_allow_html=True,
)

display_df = filtered_df[
    [
        "player_name",
        "team_name",
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
        "player_name": "Player",
        "team_name": "Team",
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

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Matches": st.column_config.NumberColumn("Matches", format="%d"),
        "PV+": st.column_config.NumberColumn("PV+", format="%.2f"),
        "Passing": st.column_config.NumberColumn("Passing", format="%.2f"),
        "Receiving": st.column_config.NumberColumn("Receiving", format="%.2f"),
        "Carrying": st.column_config.NumberColumn("Carrying", format="%.2f"),
        "Shooting": st.column_config.NumberColumn("Shooting", format="%.2f"),
        "Defending": st.column_config.NumberColumn("Defending", format="%.2f"),
    },
)

st.markdown("</div>", unsafe_allow_html=True)
