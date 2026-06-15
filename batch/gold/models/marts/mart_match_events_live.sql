WITH events AS (
    SELECT * FROM {{ ref('fact_game_events') }}
),

clubs AS (
    SELECT sk_club, club_name FROM {{ ref('dim_club') }}
),

dates AS (
    SELECT sk_date, full_date FROM {{ ref('dim_date') }}
)

SELECT
    e.game_id,
    c.club_name,
    d.full_date                               AS event_date,
    e.minute,
    e.event_type,
    e.description,
    e.player_id,
    e.source_type,

    -- Running totals per match per club
    COUNT(CASE WHEN e.event_type = 'goals' THEN 1 END)
        OVER (PARTITION BY e.game_id, e.club_id)        AS goals_in_match,
    COUNT(CASE WHEN e.event_type = 'cards' THEN 1 END)
        OVER (PARTITION BY e.game_id, e.club_id)        AS cards_in_match,
    COUNT(CASE WHEN e.event_type = 'substitutions' THEN 1 END)
        OVER (PARTITION BY e.game_id, e.club_id)        AS subs_in_match

FROM events e
LEFT JOIN clubs c ON e.sk_club = c.sk_club
LEFT JOIN dates d ON e.sk_date = d.sk_date