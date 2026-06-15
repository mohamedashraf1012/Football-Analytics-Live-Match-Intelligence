-- dim_date is generated entirely from SQL — no source table needed
-- It covers the full date range found in the games table


WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart   
= "day",
        start_date = "CAST('2000-01-01' AS DATE)",
        end_date   
= "CAST('2030-12-31' AS DATE)"
    ) }}
),

dates AS (
SELECT
        date_day AS full_date
FROM date_spine
)

SELECT
-- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS sk_date,
-- Natural key
 full_date AS date_id,

 -- Date parts
  full_date,
  YEAR(full_date) AS year,
  MONTH(full_date) AS month,
  DAY(full_date) AS day,
  DAYOFWEEK(full_date) AS day_of_week,
  DAYNAME(full_date) AS day_name,
  MONTHNAME(full_date) AS month_name,
  QUARTER(full_date) AS quarter,
  WEEKOFYEAR(full_date) AS week_of_year,

--  Football season (season starts in July/August
CASE
    WHEN MONTH(full_date) >= 7
    THEN CONCAT(YEAR(full_date), '/', RIGHT(CAST(YEAR(full_date)+1 AS VARCHAR), 2))
    ELSE CONCAT(YEAR(full_date)-1, '/', RIGHT(CAST(YEAR(full_date) AS VARCHAR), 2))
END AS season_label,

CASE 
    WHEN MONTH(full_date) >= 7
    THEN YEAR(full_date)
    ELSE YEAR(full_date)-1
END AS season_start_year

FROM dates
