WITH source AS (
    SELECT * FROM {{ source('FOOTBALL_DE', 'RAW_GAMES') }}
)

SELECT
    game_id,
    competition_id,
    season,
    CONCAT(CAST(season AS VARCHAR), '/', RIGHT(CAST(season + 1 AS VARCHAR), 2)) AS season_label,
    round,
    date as game_date,
    home_club_id,
    away_club_id,
    home_club_goals,
    away_club_goals,
    home_club_position,
    away_club_position,
    home_club_manager_name,
    away_club_manager_name,
    home_club_formation,
    away_club_formation,
    home_club_name,
    away_club_name,
    aggregate,
    competition_type,
    CURRENT_TIMESTAMP()  AS Loaded_at                     
FROM source
WHERE game_id IS NOT NULL