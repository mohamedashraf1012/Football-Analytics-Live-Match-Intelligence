WITH appearances AS (
    SELECT
        *
    FROM
        {{ ref('fact_appearances') }}
),
players AS (
    SELECT
        sk_player,
        player_full_name,
        POSITION,
        nationality,
        current_club_name,
        age,
        is_active,
        is_international,
        latest_market_value_eur
    FROM
        {{ ref('dim_player') }}
),
dates AS (
    SELECT
        sk_date,
        YEAR,
        season_label,
        season_start_year
    FROM
        {{ ref('dim_date') }}
),
clubs AS (
    SELECT
        sk_club,
        club_name
    FROM
        {{ ref('dim_club') }}
)
SELECT
    p.player_full_name,
    p.position,
    p.nationality,
    p.current_club_name,
    p.age,
    p.is_active,
    p.is_international,
    p.latest_market_value_eur,
    d.season_label AS season,
    d.season_start_year,
    C.club_name AS club_at_time,
     --  Aggregated measures
    COUNT(
        A.appearance_id
    ) AS total_appearances,
    SUM(
        A.goals
    ) AS total_goals,
    SUM(
        A.assists
    ) AS total_assists,
    SUM(
        A.minutes_played
    ) AS total_minutes,
    SUM(
        A.yellow_cards
    ) AS total_yellow_cards,
    SUM(
        A.red_cards
    ) AS total_red_cards,
    ROUND(AVG(A.goals), 2) AS avg_goals_per_match,
    ROUND(AVG(A.assists), 2) AS avg_assists_per_match,
    ROUND(AVG(A.minutes_played), 0) AS avg_minutes_per_match,
    ROUND(SUM(A.goals) * 90.0 / NULLIF(SUM(A.minutes_played), 0), 2) AS goals_per_90_min
FROM
    appearances A
    JOIN players p
    ON A.sk_player = p.sk_player
    JOIN dates d
    ON A.sk_date = d.sk_date
    JOIN clubs C
    ON A.sk_club = C.sk_club
GROUP BY
    p.player_full_name,
    p.position,
    p.nationality,
    p.current_club_name,
    p.age,
    p.is_active,
    p.is_international,
    p.latest_market_value_eur,
    d.season_label,
    d.season_start_year,
    C.club_name
