WITH
shops AS (
    SELECT
        shop_subdomain
    FROM {{ ref('shops') }}
    WHERE is_mql
    AND NOT IS_ZOMBIE_SHOPIFY_PLAN
),

calendar AS (
    SELECT
        DISTINCT DATE_TRUNC(week, date_day) AS dt
    FROM {{ ref('calendar_dates') }}
),

ends_of_weeks AS (
    SELECT
        DATEADD(day, 6, dt)
     FROM calendar
),

shop_days AS (
    SELECT
        shop_subdomain,
        dt,
        inc_amount
     FROM {{ ref('mesa_shop_days') }}
     LEFT JOIN shops USING (shop_subdomain)

     WHERE inc_amount > 0

),

first_shop_weeks AS (
    SELECT
        shop_subdomain,
        DATE_TRUNC(week, MIN(dt)) AS cohort_week
    FROM shop_days
    GROUP BY 1
),

decorated_first_shop_weeks AS (
    SELECT
        shop_subdomain,
        cohort_week,
        AVG(inc_amount) * 30 AS mrr
    FROM shop_days
    LEFT JOIN first_shop_weeks USING (shop_subdomain)
    WHERE DATE_PART(year, cohort_week) = DATE_PART(year, dt)
        AND DATE_PART(week, cohort_week) = DATE_PART(week, dt)
    GROUP BY 1, 2
),

cohort_info AS (
   SELECT
        cohort_week,
        COUNT(DISTINCT shop_subdomain) AS cohort_size,
        ROUND(SUM(mrr)) AS cohort_starting_mrr
    FROM decorated_first_shop_weeks
    GROUP BY 1
),

shop_weeks AS (
    SELECT
        DISTINCT DATE_TRUNC(week, dt) AS period_week,
        shop_subdomain,
        first_shop_weeks.cohort_week,
        inc_amount * 30 AS week_mrr

     FROM shop_days
     LEFT JOIN first_shop_weeks USING (shop_subdomain)
     WHERE dt IN (SELECT * FROM ends_of_weeks)
),

shop_week_activity AS (
    SELECT
        cohort_week,
        period_week,
        COUNT(DISTINCT shop_subdomain) AS retained_shops,
        SUM(week_mrr) AS retained_mrr
    FROM shop_weeks
    GROUP BY 1, 2
)

SELECT
    cohort_week,
    CONCAT(cohort_week, ' [', cohort_size, ' / ', to_varchar(cohort_starting_mrr, '$9,000'), ']') AS cohort_info,
    FLOOR(DATEDIFF(week, cohort_week, period_week)) as period,
    retained_shops,
    retained_shops / cohort_size::float AS retention_rate,
    retained_mrr,
    retained_mrr / cohort_starting_mrr AS revenue_retention_rate
FROM cohort_info


LEFT JOIN shop_week_activity USING (cohort_week)
WHERE period IS NOT NULL
AND cohort_week > CURRENT_DATE - INTERVAL '60 weeks'
ORDER BY 1, 2
