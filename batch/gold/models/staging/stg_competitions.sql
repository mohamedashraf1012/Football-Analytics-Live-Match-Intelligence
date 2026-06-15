WITH source AS (
    SELECT * FROM {{ source('FOOTBALL_DE', 'RAW_COMPETITIONS') }}
)

SELECT
    competition_id,
    name                        AS competition_name,                               
    sub_type,
    type                        AS competition_type,                                     
    country_id,
    country_name,
    domestic_league_code,
    confederation,
    total_clubs,
    CURRENT_TIMESTAMP()         AS Loaded_at                     
FROM source
WHERE competition_id IS NOT NULL
