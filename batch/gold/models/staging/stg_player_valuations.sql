WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'FOOTBALL_DE',
            'RAW_PLAYER_VALUATIONS'
        ) }}
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY player_id
            ORDER BY
                DATE DESC
        ) AS rn
    FROM
        source
    WHERE
        market_value_in_eur IS NOT NULL
)
SELECT
    player_id,
    DATE AS valuation_date,
    market_value_in_eur,
    current_club_name,
    current_club_id,
    player_club_domestic_competition_id,
    rn,
    CASE
        WHEN rn = 1 THEN TRUE
        ELSE FALSE
    END AS is_latest_valuation,
    CURRENT_TIMESTAMP() AS loaded_at
FROM
    ranked
