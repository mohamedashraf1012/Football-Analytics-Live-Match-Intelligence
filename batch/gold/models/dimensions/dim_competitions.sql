WITH competitions AS (
    SELECT * FROM {{ ref('stg_competitions') }}
)
SELECT
    -- surrogate key
    {{ dbt_utils.generate_surrogate_key(['competition_id']) }} AS sk_competition,
    -- NATURAL key 
    competition_id,
    -- competition attributes 
    competition_name,
    sub_type,
    competition_type,
    country_id,
    country_name,
    domestic_league_code,
    confederation,
    total_clubs,
    -- metadata 
    CURRENT_TIMESTAMP() AS updated_at
FROM
    competitions
