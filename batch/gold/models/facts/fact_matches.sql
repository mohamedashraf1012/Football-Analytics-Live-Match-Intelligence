{{
    config(
        materialized='incremental',
        unique_key='game_id',
        incremental_strategy='merge'
    )
}}

WITH games AS (
    SELECT * FROM {{ ref('stg_games') }}
),

home_club AS (
SELECT sk_club AS home_club_key, club_id FROM {{ ref('dim_club') }}
),

away_club AS (
SELECT sk_club AS away_club_key, club_id FROM {{ ref('dim_club') }}
),

dim_competition AS (
SELECT sk_competition AS competition_key, competition_id FROM {{ ref('dim_competitions') }}
),

dim_date AS (
SELECT sk_date AS date_key, date_id FROM {{ ref('dim_date') }}
)


SELECT
-- Natural key
    g.game_id,
-- Foreign keys (surrogate)
    hc.home_club_key,
    ac.away_club_key,
    dc.competition_key,
    dd.date_key,
-- Natural foreign keys
    g.home_club_id,
    g.away_club_id,
    g.competition_id,
    g.game_date,
    -- Match attributes
    g.season,
    g.season_label,
    g.round,  
     g.competition_type,
    g.aggregate,
    g.home_club_name,
    g.away_club_name,
    g.home_club_manager_name,
    g.away_club_manager_name,
    g.home_club_formation,
    g.away_club_formation,
    g.home_club_position,
    g.away_club_position,
    -- Measures
    g.home_club_goals       AS home_goals,                 
    g.away_club_goals       AS away_goals,                     
    (g.home_club_goals + g.away_club_goals)  AS total_goals,
    (g.home_club_goals - g.away_club_goals)  AS goal_difference,

-- Win / loss flags
CASE WHEN g.home_club_goals > g.away_club_goals THEN TRUE ELSE FALSE END AS home_win,
CASE WHEN g.home_club_goals < g.away_club_goals THEN TRUE ELSE FALSE END AS away_win,
CASE WHEN g.home_club_goals = g.away_club_goals THEN TRUE ELSE FALSE END AS is_draw,

-- Metadata
    g.loaded_at
FROM games g
LEFT JOIN home_club hc ON g.home_club_id = hc.club_id
LEFT JOIN away_club ac ON g.away_club_id = ac.club_id
LEFT JOIN dim_competition dc ON g.competition_id = dc.competition_id
LEFT JOIN dim_date dd ON g.game_date = dd.date_id

{% if is_incremental() %}
-- مهم جدًا: filter للـ new/updated rows
WHERE g.loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}

