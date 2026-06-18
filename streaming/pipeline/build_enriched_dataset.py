import pandas as pd

# =========================
# CONFIG
# =========================

DATA_PATH = r"D:\ITI LABS\Grad project\datasets\raw_data"   # غيّر لو الـ path مختلف
OUTPUT_PATH = r"D:\ITI LABS\Grad project\datasets\raw_data\football_events_enriched2.csv"
SAMPLE_SIZE = 20
RANDOM_STATE = 42

# =========================
# 1. LOAD
# =========================

events       = pd.read_csv(f"{DATA_PATH}/game_events.csv")
players      = pd.read_csv(f"{DATA_PATH}/players.csv")
games        = pd.read_csv(f"{DATA_PATH}/games.csv")
lineups      = pd.read_csv(f"{DATA_PATH}/game_lineups.csv", dtype={"number": str}, low_memory=False)
clubs        = pd.read_csv(f"{DATA_PATH}/clubs.csv")
competitions = pd.read_csv(f"{DATA_PATH}/competitions.csv")
club_games   = pd.read_csv(f"{DATA_PATH}/club_games.csv")

# =========================
# 2. SAMPLE 20 MATCHES
# =========================

sample_game_ids = events["game_id"].drop_duplicates().sample(SAMPLE_SIZE, random_state=RANDOM_STATE)

events     = events[events["game_id"].isin(sample_game_ids)]
games      = games[games["game_id"].isin(sample_game_ids)]
lineups    = lineups[lineups["game_id"].isin(sample_game_ids)]
club_games = club_games[club_games["game_id"].isin(sample_game_ids)]

# =========================
# 3. RENAME + SELECT  ← كل conflict بيتحل هنا قبل أي merge
# =========================

# --- events ---
events = events[[
    "game_event_id", "game_id", "minute",
    "type", "club_id", "player_id",
    "description", "player_in_id", "player_assist_id", "date"
]].rename(columns={
    "type": "event_type",
    "date": "event_date"
})

# --- games ---
games = games[[
    "game_id", "competition_id", "season", "round", "date",
    "home_club_id", "away_club_id",
    "home_club_goals", "away_club_goals",
    "home_club_formation", "away_club_formation",
    "home_club_name", "away_club_name",
    "stadium", "attendance", "referee"
]].rename(columns={
    "date": "game_date"
})

# --- players ---
players = players[[
    "player_id", "name", "position", "sub_position",
    "foot", "country_of_citizenship",
    "height_in_cm", "market_value_in_eur"
]].rename(columns={
    "name":                   "player_name",
    "position":               "player_position",
    "sub_position":           "player_sub_position",
    "foot":                   "player_foot",
    "country_of_citizenship": "player_nationality",
    "height_in_cm":           "player_height_cm",
    "market_value_in_eur":    "player_market_value"
})

# --- clubs ---
clubs = clubs[[
    "club_id", "name", "coach_name",
    "stadium_name", "total_market_value"
]].rename(columns={
    "name":               "club_name",
    "total_market_value": "club_total_market_value"
})

# --- competitions ---
competitions = competitions[[
    "competition_id", "name", "type",
    "country_name", "confederation"
]].rename(columns={
    "name": "competition_name",
    "type": "competition_type"
})

# --- lineups ---
lineups = lineups[[
    "game_id", "player_id",
    "type", "position", "team_captain", "number"
]].rename(columns={
    "type":     "lineup_type",
    "position": "lineup_position",
    "number":   "shirt_number"
})

# --- club_games ---
club_games = club_games[[
    "game_id", "club_id",
    "own_goals", "own_position",
    "opponent_goals", "opponent_position",
    "hosting", "is_win"
]].rename(columns={
    "own_goals":        "club_goals_scored",
    "own_position":     "club_league_position",
    "opponent_goals":   "club_goals_conceded",
    "opponent_position":"opponent_league_position"
})

# =========================
# 4. MERGE  ← بعد ما كل حاجه اتسمّت صح
# =========================

df = events.merge(games,        on="game_id",              how="left")
df = df.merge(competitions,     on="competition_id",        how="left")
df = df.merge(players,          on="player_id",             how="left")
df = df.merge(clubs,            on="club_id",               how="left")
df = df.merge(lineups,          on=["game_id", "player_id"],how="left")
df = df.merge(club_games,       on=["game_id", "club_id"],  how="left")

# =========================
# 5. FINAL CLEANUP
# =========================

df["team_captain"] = df["team_captain"].fillna(0).astype(int)

final_cols = [
    # Event core
    "game_event_id", "game_id", "event_date", "game_date",
    "minute", "event_type", "description",
    "player_in_id", "player_assist_id",

    # Player
    "player_id", "player_name", "player_position", "player_sub_position",
    "player_foot", "player_nationality", "player_height_cm", "player_market_value",

    # Club (event owner)
    "club_id", "club_name", "coach_name", "club_total_market_value",

    # Lineup
    "lineup_type", "lineup_position", "shirt_number", "team_captain",

    # Club game stats
    "club_goals_scored", "club_goals_conceded",
    "club_league_position", "opponent_league_position",
    "hosting", "is_win",

    # Competition
    "competition_id", "competition_name", "competition_type",
    "country_name", "confederation",

    # Match info
    "season", "round", "stadium", "attendance", "referee",
    "home_club_id", "away_club_id",
    "home_club_name", "away_club_name",
    "home_club_goals", "away_club_goals",
    "home_club_formation", "away_club_formation"
]

# safeguard: بس الأعمدة الموجودة فعلاً
final_cols = [c for c in final_cols if c in df.columns]
df_final   = df[final_cols]

# =========================
# 6. SAVE
# =========================

df_final.to_csv(OUTPUT_PATH, index=False)

print("✅ Done!")
print(f"Rows   : {len(df_final):,}")
print(f"Columns: {len(df_final.columns)}")
print(f"Saved  : {OUTPUT_PATH}")
print("\nColumns list:")
for col in df_final.columns:
    print(f"  {col}")
