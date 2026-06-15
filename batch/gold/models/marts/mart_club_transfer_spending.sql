WITH transfers AS (
    SELECT * FROM {{ ref('fact_transfers') }}
),

clubs AS (
    SELECT sk_club, club_name FROM {{ ref('dim_club') }}
),

players AS (
    SELECT sk_player, player_full_name, position FROM {{ ref('dim_player') }}
),

dates AS (
    SELECT sk_date, season_label, year FROM {{ ref('dim_date') }}
)

SELECT
    c_to.club_name                            AS buying_club,
    c_from.club_name                          AS selling_club,
    p.player_full_name,
    p.position,
    d.season_label                        AS transfer_season,

    t.transfer_fee_eur,
    t.player_value_at_transfer_eur,
    t.transfer_fee_type,
    t.transfer_date,

    -- Aggregates per buying club per season
    SUM(t.transfer_fee_eur) OVER (
        PARTITION BY t.sk_to_club, d.season_label
    )                                         AS total_spent_that_season,

    SUM(t.transfer_fee_eur) OVER (
        PARTITION BY t.sk_from_club, d.season_label
    )                                         AS total_received_that_season

FROM transfers t
LEFT JOIN clubs   c_to   ON t.sk_to_club   = c_to.sk_club
LEFT JOIN clubs   c_from ON t.sk_from_club = c_from.sk_club
LEFT JOIN players p      ON t.sk_player    = p.sk_player
LEFT JOIN dates   d      ON t.sk_date      = d.sk_date