WITH transfers AS (
    SELECT * FROM {{ ref('stg_transfers') }}
),
dim_player AS (
    SELECT sk_player, player_id FROM {{ ref('dim_player') }}
),
from_club AS (
    SELECT sk_club AS sk_from_club, club_id FROM {{ ref('dim_club') }}
),
to_club AS (
    SELECT sk_club AS sk_to_club, club_id FROM {{ ref('dim_club') }}
),
dim_date AS (
    SELECT sk_date, date_id FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['t.player_id', 't.from_club_id', 't.to_club_id', 't.transfer_date']) }} AS transfer_key,
    -- Foreign keys (surrogate) — NULL لو النادي مش في dim_club
    dp.sk_player,
    fc.sk_from_club,
    tc.sk_to_club,
    dd.sk_date,

    -- Natural foreign keys — موجودة دايماً
    t.player_id,
    t.from_club_id,
    t.to_club_id,
    t.transfer_date,

    -- -- Club names — COALESCE: لو dim_club عنده الاسم استخدمه، لو لأ استخدم اللي في الـ transfer
    -- COALESCE(fc.from_club_name_dim, t.from_club_name) AS from_club_name,
    -- COALESCE(tc.to_club_name_dim,   t.to_club_name)   AS to_club_name,

    -- Attributes
    t.transfer_season,
    t.from_club_name,
    t.to_club_name,
    t.player_name,

    -- Measures
    t.transfer_fee_eur,
    t.player_value_at_transfer_eur,
    t.transfer_fee_type,

    -- Metadata
    t.loaded_at

FROM transfers t
LEFT JOIN dim_player dp ON t.player_id    = dp.player_id
LEFT JOIN from_club  fc ON t.from_club_id = fc.club_id
LEFT JOIN to_club    tc ON t.to_club_id   = tc.club_id
LEFT JOIN dim_date   dd ON t.transfer_date = dd.date_id