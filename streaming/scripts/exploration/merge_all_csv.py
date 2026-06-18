import pandas as pd

DATA_PATH = r"D:\ITI LABS\Grad project\datasets\raw_data"

# =========================
# 1. LOAD DATA (safe mode)
# =========================

events = pd.read_csv(f"{DATA_PATH}/game_events.csv")
players = pd.read_csv(f"{DATA_PATH}/players.csv", low_memory=False)
games = pd.read_csv(f"{DATA_PATH}/games.csv")
lineups = pd.read_csv(f"{DATA_PATH}/game_lineups.csv", low_memory=False)
clubs = pd.read_csv(f"{DATA_PATH}/clubs.csv")
competitions = pd.read_csv(f"{DATA_PATH}/competitions.csv")

print("✅ Data loaded")

# =========================
# 2. FIX COLUMN CONFLICTS
# =========================

games = games.rename(columns={"date": "game_date"})
events = events.rename(columns={"date": "event_date"})

# =========================
# 3. SAMPLE 20 MATCHES
# =========================

sample_games = events["game_id"].drop_duplicates().sample(20, random_state=42)

events = events[events["game_id"].isin(sample_games)]
games = games[games["game_id"].isin(sample_games)]
lineups = lineups[lineups["game_id"].isin(sample_games)]

print(f"✅ Sampled {len(sample_games)} matches")

# =========================
# 4. REDUCE COLUMNS (OPTIMIZATION)
# =========================

players = players[[
    "player_id", "name", "position", "sub_position",
    "foot", "country_of_citizenship",
    "height_in_cm", "market_value_in_eur"
]]

clubs = clubs[[
    "club_id", "name", "coach_name",
    "stadium_name", "total_market_value"
]]

competitions = competitions[[
    "competition_id", "name",
    "country_name", "confederation", "type"
]]

games = games[[
    "game_id", "competition_id", "game_date",
    "home_club_id", "away_club_id",
    "home_club_goals", "away_club_goals",
    "home_club_formation", "away_club_formation",
    "stadium", "attendance", "referee", "season", "round"
]]

# =========================
# 5. EVENTS + GAMES
# =========================

df = events.merge(games, on="game_id", how="left")

# =========================
# 6. COMPETITION ENRICHMENT
# =========================

df = df.merge(competitions, on="competition_id", how="left")

df = df.rename(columns={
    "name": "competition_name",
    "country_name": "competition_country",
    "type": "competition_type"
})

# =========================
# 7. PLAYER ENRICHMENT
# =========================

df = df.merge(players, on="player_id", how="left")

df = df.rename(columns={
    "name": "player_name",
    "country_of_citizenship": "player_nationality",
    "market_value_in_eur": "player_market_value",
    "height_in_cm": "player_height_cm",
    "foot": "player_foot"
})

# =========================
# 8. CLUB ENRICHMENT
# =========================

df = df.merge(clubs, on="club_id", how="left")

df = df.rename(columns={
    "name": "club_name",
    "total_market_value": "club_market_value"
})

# =========================
# 9. LINEUPS ENRICHMENT
# =========================

lineups = lineups[[
    "game_id", "player_id",
    "type", "position", "team_captain",
    "number"
]]

df = df.merge(lineups, on=["game_id", "player_id"], how="left")

df = df.rename(columns={
    "type": "lineup_type",
    "position": "lineup_position",
    "number": "shirt_number"
})

df["team_captain"] = df["team_captain"].fillna(0).astype(int)

# =========================
# 10. FINAL FIX (IMPORTANT)
# =========================

df["event_date"] = df["event_date"].fillna(df["game_date"])

# =========================
# 11. FINAL COLUMN ORDER
# =========================

final_cols = [
    "game_event_id", "game_id", "event_date", "minute", "type", "description",

    "player_id", "player_name", "player_position", "sub_position",
    "player_foot", "player_nationality", "player_height_cm", "player_market_value",

    "club_id", "club_name", "coach_name", "club_market_value",

    "lineup_type", "lineup_position", "shirt_number", "team_captain",

    "competition_id", "competition_name", "competition_country",
    "confederation", "competition_type",

    "season", "round", "stadium", "attendance", "referee",

    "home_club_id", "away_club_id",
    "home_club_goals", "away_club_goals",
    "home_club_formation", "away_club_formation"
]

final_cols = [c for c in final_cols if c in df.columns]

df_final = df[final_cols]

# =========================
# 12. SAVE OUTPUT
# =========================

output_path = f"{DATA_PATH}/football_events_enriched.csv"
df_final.to_csv(output_path, index=False)

print("\n🎉 DONE SUCCESSFULLY!")
print("Rows:", len(df_final))
print("Columns:", len(df_final.columns))
print("Saved to:", output_path)