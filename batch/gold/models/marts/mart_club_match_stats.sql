WITH matches AS (
    SELECT * FROM {{ ref('fact_matches') }}
),

dim_club AS (
    SELECT sk_club, club_id, club_name FROM {{ ref('dim_club') }}
),

dim_competition AS (
    SELECT sk_competition AS competition_key, competition_id, competition_name, competition_type
    FROM {{ ref('dim_competitions') }}
),

dim_date AS (
    SELECT sk_date AS date_key, season_label, year FROM {{ ref('dim_date') }}
),

-- Unpivot each match into two club-perspective rows
club_view AS (
    -- Home club perspective
    SELECT
        m.game_id,
        m.home_club_key                        AS club_key,
        m.home_club_id                         AS club_id,
        m.competition_key,
        m.competition_id,
        m.date_key,
        'Home'                                 AS hosting,
        m.home_win                             AS is_win,
        CASE WHEN m.home_win THEN 'WIN' WHEN m.is_draw THEN 'DRAW' ELSE 'LOSS' END AS result,
        m.home_goals                           AS goals_scored,
        m.away_goals                           AS goals_conceded,
        m.goal_difference
    FROM matches m

    UNION ALL

    -- Away club perspective
    SELECT
        m.game_id,
        m.away_club_key                        AS club_key,
        m.away_club_id                         AS club_id,
        m.competition_key,
        m.competition_id,
        m.date_key,
        'Away'                                 AS hosting,
        m.away_win                             AS is_win,
        CASE WHEN m.away_win THEN 'WIN' WHEN m.is_draw THEN 'DRAW' ELSE 'LOSS' END AS result,
        m.away_goals                           AS goals_scored,
        m.home_goals                           AS goals_conceded,
        (m.away_goals - m.home_goals)          AS goal_difference
    FROM matches m
)

SELECT
    c.club_name,
    comp.competition_name,
    comp.competition_type,
    d.season_label                          AS season,
    cv.hosting,

    -- Aggregated match stats
    COUNT(cv.game_id)                          AS total_matches,
    SUM(CASE WHEN cv.is_win THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN cv.result = 'DRAW' THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN cv.result = 'LOSS' THEN 1 ELSE 0 END) AS losses,
    ROUND(SUM(CASE WHEN cv.is_win THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS win_rate_pct,
    SUM(cv.goals_scored)                       AS total_goals_scored,
    SUM(cv.goals_conceded)                     AS total_goals_conceded,
    ROUND(AVG(cv.goals_scored), 2)             AS avg_goals_scored,
    ROUND(AVG(cv.goals_conceded), 2)           AS avg_goals_conceded,
    SUM(cv.goal_difference)                    AS total_goal_difference

FROM club_view cv
LEFT JOIN dim_club        c    ON cv.club_id        = c.club_id
LEFT JOIN dim_competition comp ON cv.competition_id = comp.competition_id
LEFT JOIN dim_date        d    ON cv.date_key        = d.date_key

GROUP BY
    c.club_name, comp.competition_name,
    comp.competition_type, d.season_label,
    cv.hosting