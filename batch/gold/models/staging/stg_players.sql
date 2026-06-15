WITH source AS (
    SELECT * FROM {{ source('FOOTBALL_DE', 'RAW_PLAYERS') }}
)


SELECT
    player_id,
    first_name,
    last_name,
    name                            AS player_full_name,                           
    last_season,
    current_club_id,
    country_of_birth,
    country_of_citizenship          AS nationality,              
    date_of_birth,
    sub_position,
    position,
    foot                            AS preferred_foot,                             
    height_in_cm,
    contract_expiration_date,
    international_caps,
    international_goals,
    current_club_domestic_competition_id,
    current_club_name,
    MARKET_VALUE_IN_EUR             AS current_market_value_in_eur,                    
    highest_market_value_in_eur,
    age,
    is_international,
    is_active,
    CURRENT_TIMESTAMP()             AS Loaded_at                      
FROM source
WHERE player_id IS NOT NULL