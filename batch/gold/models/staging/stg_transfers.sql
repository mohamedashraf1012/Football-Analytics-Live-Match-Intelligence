WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'FOOTBALL_DE',
            'RAW_TRANSFERS'
        ) }}
)
SELECT
    player_id,
    transfer_date,
    transfer_season,
    from_club_id,
    to_club_id,
    from_club_name,
    to_club_name,
    COALESCE(
        transfer_fee,
        0
    ) AS transfer_fee_eur,
    market_value_in_eur AS player_value_at_transfer_eur,
    player_name,
    CASE
        WHEN transfer_fee IS NULL
        OR transfer_fee = 0 THEN 'FREE_OR_UNKNOWN'
        ELSE 'PAID'
    END AS transfer_fee_type,
    CURRENT_TIMESTAMP() AS loaded_at
FROM
    source
WHERE
    player_id IS NOT NULL
