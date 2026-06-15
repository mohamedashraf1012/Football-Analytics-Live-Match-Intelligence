WITH clubs AS (
    SELECT
        *
    FROM
        {{ ref('stg_clubs') }}
)
SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['club_id']) }} AS sk_club,
    -- Natural key
    club_id,
    -- Club attributes
    club_name,
    domestic_competition_id,
    squad_size,
    average_age,
    foreigners_number,
    foreigners_percentage,
    national_team_players,
    stadium_name,
    stadium_seats,
    last_season,
    net_transfer_record_eur,
-- Metadata
CURRENT_TIMESTAMP() AS updated_at
FROM clubs


-- WITH clubs_from_staging AS (
--     SELECT
--         club_id,
--         club_name,
--         domestic_competition_id,
--         squad_size,
--         average_age,
--         foreigners_number,
--         foreigners_percentage,
--         national_team_players,
--         stadium_name,
--         stadium_seats,
--         last_season,
--         net_transfer_record_eur
--     FROM {{ ref('stg_clubs') }}
-- ),

-- -- 1. استخراج كل الأندية الفريدة التي ظهرت كـ "نادي منقول منه" أو "نادي منقول إليه"
-- clubs_extracted_from_transfers AS (
--     SELECT DISTINCT 
--         from_club_id AS club_id, 
--         from_club_name AS club_name 
--     FROM {{ ref('stg_transfers') }}
--     WHERE from_club_id IS NOT NULL

--     UNION

--     SELECT DISTINCT 
--         to_club_id AS club_id, 
--         to_club_name AS club_name 
--     FROM {{ ref('stg_transfers') }}
--     WHERE to_club_id IS NOT NULL
-- ),

-- -- 2. دمج المصدرين بواسطة FULL OUTER JOIN لضمان عدم سقوط أي نادٍ
-- all_clubs_combined AS (
--     SELECT 
--         COALESCE(s.club_id, t.club_id) AS club_id,
--         COALESCE(s.club_name, t.club_name) AS club_name,
--         s.domestic_competition_id,
--         s.squad_size,
--         s.average_age,
--         s.foreigners_number,
--         s.foreigners_percentage,
--         s.national_team_players,
--         s.stadium_name,
--         s.stadium_seats,
--         s.last_season,
--         s.net_transfer_record_eur
--     FROM clubs_from_staging s
--     FULL OUTER JOIN clubs_extracted_from_transfers t ON s.club_id = t.club_id
-- ),

-- unique_clubs AS (
--     SELECT *
--     FROM (
--         SELECT *,
--             ROW_NUMBER() OVER (PARTITION BY club_id ORDER BY club_id) AS rn
--         FROM all_clubs_combined
--     )
--     WHERE rn = 1
-- )

-- SELECT
--     -- توليد الـ Surrogate Key بناءً على قائمة الأندية الشاملة والمحدثة
--     {{ dbt_utils.generate_surrogate_key(['club_id']) }} AS sk_club,
--     club_id,
--     club_name,
--     domestic_competition_id,
--     squad_size,
--     average_age,
--     foreigners_number,
--     foreigners_percentage,
--     national_team_players,
--     stadium_name,
--     stadium_seats,
--     last_season,
--     net_transfer_record_eur,
--     CURRENT_TIMESTAMP() AS updated_at
-- FROM unique_clubs