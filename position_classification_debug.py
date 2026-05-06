from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres.vvwfcbbyddyodkjuwdbq",
    "password": "BKBKrhdP42zt0Jo2",
    "host": "aws-1-us-east-2.pooler.supabase.com",
    "port": 5432,
}

OUT = Path("artifacts/position_classification_debug_pitch.png")
OUT.parent.mkdir(parents=True, exist_ok=True)


def position_group(position: str) -> str:
    position = (position or "").upper()
    if "GK" in position or "GOAL" in position:
        return "GK"
    if any(token in position for token in ["CB", "BACK", "DEF", "DC", "DL", "DR", "DM"]):
        return "DEF"
    if any(token in position for token in ["FW", "ST", "RW", "LW", "FORW", "WING"]):
        return "FWD"
    return "MID"


def map_custom_position_from_profile(
    avg_x: float,
    avg_y: float,
    avg_wide_offset: float,
    central_action_share: float,
    wide_action_share: float,
    raw_position: str,
    pv_defending: float,
    pv_shooting: float,
    pv_passing: float,
    pv_receiving: float,
    pv_carrying: float,
) -> str:
    """
    Horizontal pitch assumption: goal is to the right.
    x: depth (left=deeper), y: width (middle=center lane).
    """
    x = pd.to_numeric(avg_x, errors="coerce")
    y = pd.to_numeric(avg_y, errors="coerce")

    if pd.isna(x) or pd.isna(y):
        return "Central Midfielders"

    central_band = 35 <= y <= 65
    wide_band = not central_band

    wide_offset = float(pd.to_numeric(avg_wide_offset, errors="coerce") or 0.0)
    central_share = float(pd.to_numeric(central_action_share, errors="coerce") or 0.0)
    wide_share = float(pd.to_numeric(wide_action_share, errors="coerce") or 0.0)

    pos_upper = (raw_position or "").upper()

    shoot = float(pd.to_numeric(pv_shooting, errors="coerce") or 0.0)
    defend = float(pd.to_numeric(pv_defending, errors="coerce") or 0.0)
    passv = float(pd.to_numeric(pv_passing, errors="coerce") or 0.0)
    recv = float(pd.to_numeric(pv_receiving, errors="coerce") or 0.0)
    carry = float(pd.to_numeric(pv_carrying, errors="coerce") or 0.0)
    att_total = max(1e-9, shoot + passv + recv + carry)
    total_with_def = max(1e-9, shoot + passv + recv + carry + max(defend, 0.0))
    shooting_share = shoot / att_total
    defending_share = max(defend, 0.0) / total_with_def

    # 1) Defensive line split exactly by your rule
    # Center backs: left/deep x and central y
    # Outside backs: left/deep x and wide y
    if x < 45:
        # Expanded CB width band: y in [30, 70] counts as center back.
        if 30 <= y <= 70:
            return "Central Defenders"
        return "Wide Defenders"

    # 2) Strong striker anchors
    if any(t in pos_upper for t in ["FW", "ST", "CF"]):
        return "Central Forwards" if central_band else "Wide Forwards"
    if (x >= 56 and shooting_share >= 0.30) or (x >= 50 and shoot >= 1.0):
        return "Central Forwards" if central_band else "Wide Forwards"

    # 3) Midfield split rule (explicit):
    # - central x + central y -> Central Midfielders
    # - central x + wide y    -> Wide Midfielders
    if 45 <= x <= 70:
        if central_band:
            return "Central Midfielders"
        return "Wide Midfielders"

    # 4) Non-midfield fallback
    if x > 70:
        return "Central Forwards" if central_band else "Wide Forwards"

    if x <= 58 and defending_share >= 0.14:
        return "Wide Defenders"
    return "Central Midfielders" if central_band else "Wide Midfielders"


def main():
    q = """
    WITH matches_2026 AS (
        SELECT match_id
        FROM public.matches
        WHERE match_date >= DATE '2026-01-01'
          AND match_date < DATE '2027-01-01'
    ),
    match_events_2026 AS (
        SELECT
            e.player_id,
            e.team_id,
            COALESCE(e.total_mins, 0) AS total_mins,
            COALESCE(e.x, 50) AS x,
            COALESCE(e.y, 50) AS y,
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
        SELECT player_id, team_id, MAX(total_mins) AS match_minutes
        FROM match_events_2026
        GROUP BY player_id, team_id
    ),
    event_agg AS (
        SELECT
            player_id,
            team_id,
            SUM(gplus_passing) AS pv_passing,
            SUM(gplus_receiving) AS pv_receiving,
            SUM(gplus_carrying) AS pv_carrying,
            SUM(gplus_shooting) AS pv_shooting,
            SUM(gplus_defending) AS pv_defending,
            AVG(x) AS avg_x,
            AVG(y) AS avg_y,
            AVG(ABS(y - 50.0)) AS avg_wide_offset,
            AVG(CASE WHEN y BETWEEN 35 AND 65 THEN 1.0 ELSE 0.0 END) AS central_action_share,
            AVG(CASE WHEN y < 30 OR y > 70 THEN 1.0 ELSE 0.0 END) AS wide_action_share
        FROM match_events_2026
        GROUP BY player_id, team_id
    ),
    minutes_agg AS (
        SELECT player_id, COALESCE(SUM(match_minutes), 0) AS minutes_played
        FROM player_match_minutes
        GROUP BY player_id
    )
    SELECT
        e.player_id,
        p.player_name,
        p.position,
        t.team_name,
        COALESCE(m.minutes_played, 0) AS minutes_played,
        e.pv_passing,
        e.pv_receiving,
        e.pv_carrying,
        e.pv_shooting,
        e.pv_defending,
        e.avg_x,
        e.avg_y,
        e.avg_wide_offset,
        e.central_action_share,
        e.wide_action_share
    FROM event_agg e
    LEFT JOIN player_lookup p ON p.player_id_raw = e.player_id::text
    LEFT JOIN team_lookup t ON t.team_id_raw = e.team_id::text
    LEFT JOIN minutes_agg m ON m.player_id = e.player_id
    ;
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(q, conn)

    for col in ["minutes_played", "avg_x", "avg_y", "avg_wide_offset", "central_action_share", "wide_action_share",
                "pv_defending", "pv_shooting", "pv_passing", "pv_receiving", "pv_carrying"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["position_group"] = df["position"].apply(position_group)
    df = df[df["position_group"] != "GK"].copy()

    df["position_custom"] = df.apply(
        lambda r: map_custom_position_from_profile(
            r["avg_x"], r["avg_y"], r["avg_wide_offset"],
            r["central_action_share"], r["wide_action_share"],
            r["position"], r["pv_defending"], r["pv_shooting"],
            r["pv_passing"], r["pv_receiving"], r["pv_carrying"],
        ),
        axis=1,
    )

    # Optional quick inspection table for unexpected mappings
    inspect_cols = ["player_name", "team_name", "position", "position_custom", "minutes_played",
                    "avg_x", "avg_y", "avg_wide_offset", "central_action_share", "wide_action_share", "pv_defending"]
    print(df.sort_values("minutes_played", ascending=False)[inspect_cols].head(60).to_string(index=False))

    role_colors = {
        "Central Defenders": "#2563eb",
        "Wide Defenders": "#06b6d4",
        "Central Midfielders": "#14b8a6",
        "Central Midfielders": "#22c55e",
        "Central Midfielders": "#eab308",
        "Wide Midfielders": "#f97316",
        "Wide Forwards": "#ef4444",
        "Central Forwards": "#a855f7",
    }

    fig, ax = plt.subplots(figsize=(14, 9), facecolor="#f8fafc")
    ax.set_facecolor("#f8fafc")
    ax.plot([0, 100], [0, 0], color="#94a3b8", lw=1.2)
    ax.plot([0, 100], [100, 100], color="#94a3b8", lw=1.2)
    ax.plot([0, 0], [0, 100], color="#94a3b8", lw=1.2)
    ax.plot([100, 100], [0, 100], color="#94a3b8", lw=1.2)
    ax.plot([50, 50], [0, 100], color="#cbd5e1", lw=1.0, linestyle="--")

    for role, g in df.groupby("position_custom"):
        ax.scatter(g["avg_x"], g["avg_y"], s=65, alpha=0.86,
                   c=role_colors.get(role, "#334155"), edgecolors="white", linewidths=0.6, label=role)

    labels = df.sort_values("minutes_played", ascending=False).head(45)
    for _, r in labels.iterrows():
        ax.text(r["avg_x"] + 0.9, r["avg_y"] + 0.5, str(r["player_name"]).split(" ")[-1], fontsize=7.3, color="#0f172a")

    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_xlabel("Average Action Depth (x)")
    ax.set_ylabel("Average Action Width (y)")
    ax.set_title("MLS 2026 Position Classification Debug Pitch", fontsize=14, fontweight="bold")
    ax.grid(False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=4, frameon=False, fontsize=8)
    plt.tight_layout()
    fig.savefig(OUT, dpi=240, bbox_inches="tight")
    print(f"\nSaved debug pitch: {OUT.resolve()}")


if __name__ == "__main__":
    main()
