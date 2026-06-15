-- no increnemtal because this is chnages in valautions of player nedd to see changes

WITH valuations AS (
    SELECT * FROM {{ ref('stg_player_valuations') }}
),

dim_player AS (
SELECT sk_player, player_id FROM {{ ref('dim_player') }}
),
dim_club AS (
SELECT sk_club, club_id FROM {{ ref('dim_club') }}
),
dim_date AS (
SELECT sk_date, date_id FROM {{ ref('dim_date') }}
)

SELECT
-- Foreign keys (surrogate)
    dp.sk_player,
    dc.sk_club,
    dd.sk_date,
-- Natural foreign keys
    v.player_id,
    v.current_club_id,
    v.valuation_date,
-- Measures
    v.market_value_in_eur AS market_value_eur,
   -- Attributes
    v.current_club_name,
    v.player_club_domestic_competition_id,
    v.is_latest_valuation,
-- Metadata
    v.loaded_at 

FROM valuations v
LEFT JOIN dim_player dp ON v.player_id = dp.player_id
LEFT JOIN dim_club dc ON v.current_club_id = dc.club_id 
LEFT JOIN dim_date dd ON v.valuation_date = dd.date_id

