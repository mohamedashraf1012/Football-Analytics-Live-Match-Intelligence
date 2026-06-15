WITH source AS (
    SELECT * FROM {{ source('FOOTBALL_DE', 'RAW_APPEARANCES') }}
)

SELECT
    appearance_id,
    game_id,
    player_id,
    player_club_id,
    player_current_club_id,
    date AS appearance_date,
    player_name,
    competition_id,
    COALESCE(goals, 0)      AS goals,                  
    COALESCE(assists, 0)     AS assists,                     
    COALESCE(minutes_played, 0) AS minutes_played,              
    COALESCE(yellow_cards, 0) AS yellow_cards,                
    COALESCE(red_cards, 0)    AS red_cards,
    CASE WHEN goals > 0 THEN TRUE ELSE FALSE END AS scored,
    CASE WHEN assists > 0 THEN TRUE ELSE FALSE END AS assisted,
    CURRENT_TIMESTAMP()     AS Loaded_at
FROM source
WHERE appearance_id IS NOT NULL
AND game_id IS NOT NULL
AND player_id IS NOT NULL
       