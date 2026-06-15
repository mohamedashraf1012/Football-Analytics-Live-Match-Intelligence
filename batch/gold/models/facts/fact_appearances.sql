{{
    config(
        materialized='incremental',
        unique_key='appearance_id',
        incremental_strategy='merge'
    )
}}

WITH appearances AS (
    SELECT * FROM {{ ref('stg_appearances') }}
),
dim_player AS (
    SELECT sk_player, player_id FROM {{ ref('dim_player') }}
),

dim_club AS (
    SELECT sk_club, club_id FROM {{ ref('dim_club') }}
),

dim_competition AS (
    SELECT sk_competition, competition_id FROM {{ ref('dim_competitions') }}
),
dim_date AS (
    SELECT sk_date, date_id FROM {{ ref('dim_date') }}
)

-- grain 1 player appearance in a game 
SELECT
-- Natural key
    a.appearance_id,
-- Foreign keys (surrogate)
    dp.sk_player,
    dc.sk_club,
    dcomp.sk_competition,
    dd.sk_date,
-- Natural foreign keys (kept for streaming / external joins)
    a.game_id,
    a.player_id,
    a.player_club_id,
    a.player_current_club_id,
    a.competition_id,
    a.appearance_date,
-- Measures
    a.goals,
    a.assists,
    a.minutes_played,
    a.yellow_cards,
    a.red_cards,
    a.scored,
    a.assisted,
-- Metadata
    a.loaded_at

FROM appearances a
LEFT JOIN dim_player    dp      ON a.player_id = dp.player_id
LEFT JOIN dim_club      dc      ON a.player_club_id = dc.club_id
LEFT JOIN dim_competition dcomp ON a.competition_id = dcomp.competition_id
LEFT JOIN dim_date      dd      ON a.appearance_date = dd.date_id


{% if is_incremental() %}
-- مهم جدًا: filter للـ new/updated rows
WHERE a.loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}