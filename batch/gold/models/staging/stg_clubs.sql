WITH source AS (
    SELECT * FROM {{ source('FOOTBALL_DE', 'RAW_CLUBS') }}
)

SELECT
    club_id,
    name                        AS club_name,                         
    domestic_competition_id,
    squad_size,
    average_age,
    foreigners_number,
    foreigners_percentage,
    national_team_players,
    stadium_name,
    stadium_seats,
    last_season,
    net_transfer_record_eur,
    CURRENT_TIMESTAMP()        AS Loaded_at               
FROM source
WHERE club_id IS NOT NULL