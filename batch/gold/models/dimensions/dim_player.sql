WITH players AS (
    SELECT * FROM {{ ref('stg_players') }}
    
),
latest_valuation AS (
    SELECT
        player_id,
        market_value_in_eur AS latest_market_value_eur,
        valuation_date AS latest_valuation_date
    FROM
        {{ ref('stg_player_valuations') }}
    WHERE
        is_latest_valuation = TRUE
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['p.player_id']) }} AS sk_player,
    -- Natural key
    p.player_id,
    --  Player attributes
    p.player_full_name,
    p.first_name,
    p.last_name,
    p.position,
    p.sub_position,
    p.preferred_foot,
    p.nationality,
    p.country_of_birth,
    p.date_of_birth,
    p.age,
    p.height_in_cm,
    -- Current club
    p.current_club_id,
    p.current_club_name,
    p.current_club_domestic_competition_id,
    --  Career info
    p.last_season,
    p.international_caps,
    p.international_goals,
    p.contract_expiration_date,
    --  Market value
    p.current_market_value_in_eur,
    p.highest_market_value_in_eur,
    lv.latest_market_value_eur,
    lv.latest_valuation_date,
    -- flags
    p.is_international,
    p.is_active,
    -- metadata
    CURRENT_TIMESTAMP() AS updated_at
FROM
    players p
    LEFT JOIN latest_valuation lv
    ON p.player_id = lv.player_id
