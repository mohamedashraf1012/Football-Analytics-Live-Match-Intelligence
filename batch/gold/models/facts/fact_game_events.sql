{{
    config(
        materialized='incremental',
        unique_key='game_event_id',
        incremental_strategy='merge'
    )
}}
WITH events AS (
    SELECT * FROM {{ ref('stg_game_events') }}
),
dim_club AS (
    SELECT sk_club, club_id FROM {{ ref('dim_club') }}
),
dim_date AS (
    SELECT sk_date, date_id FROM {{ ref('dim_date') }}
)
SELECT
    e.game_event_id,
    dc.sk_club,
    dd.sk_date,
    e.game_id,
    e.club_id,
    e.player_id,
    e.event_date,
    e.minute,
    e.event_type,
    e.club_name,
    e.description,
    e.source_type,
    e.loaded_at
FROM events e
LEFT JOIN dim_club dc ON e.club_id  = dc.club_id
LEFT JOIN dim_date dd ON e.event_date = dd.date_id

{% if is_incremental() %}
-- مهم جدًا: filter للـ new/updated rows
WHERE e.loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}
