WITH source AS (
    SELECT * FROM {{ source('FOOTBALL_DE', 'RAW_GAME_EVENTS') }}
)

SELECT
    game_event_id,
    date                                     AS event_date,
    game_id,
    minute,
    type                                     AS event_type,
    club_id,
    club_name,
    player_id,
    description,
    'batch'                                  AS source_type,
    CURRENT_TIMESTAMP()                      AS loaded_at
FROM source
WHERE game_event_id IS NOT NULL

