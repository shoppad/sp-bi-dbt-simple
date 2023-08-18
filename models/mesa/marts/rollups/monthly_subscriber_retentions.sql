WITH

shops AS (
    SELECT
        shop_subdomain
    FROM {{ ref('shops') }}
    WHERE is_mql
    AND NOT IS_ZOMBIE_SHOPIFY_PLAN
),

calendar AS (
    SELECT DISTINCT DATE_TRUNC(month, date_day) AS dt
    FROM {{ ref('calendar_dates') }}
),

ends_of_months AS (
    SELECT
        DISTINCT LAST_DAY(dt) AS dt
     FROM calendar
),

shop_days AS (
    SELECT
        shop_subdomain,
        dt,
        inc_amount
    FROM {{ ref('mesa_shop_days') }}
    INNER JOIN shops USING (shop_subdomain)
    WHERE inc_amount > 0
),

first_shop_months AS (
    SELECT
        shop_subdomain,
        DATE_TRUNC(month, MIN(dt)) AS cohort_month
    FROM shop_days
    GROUP BY 1
),

decorated_first_shop_months AS (
    SELECT
        shop_subdomain,
        cohort_month,
        AVG(inc_amount) * 30 AS mrr
    FROM shop_days
    LEFT JOIN first_shop_months USING (shop_subdomain)
    WHERE DATE_PART(year, cohort_month) = DATE_PART(year, dt)
        AND DATE_PART(month, cohort_month) = DATE_PART(month, dt)
    GROUP BY 1, 2
),

cohort_info AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT shop_subdomain) AS cohort_size,
        ROUND(SUM(mrr)) AS cohort_starting_mrr
    FROM decorated_first_shop_months
    GROUP BY 1
),

shop_months AS (
    SELECT
        DISTINCT DATE_TRUNC(month, dt) AS period_month,
        shop_subdomain,
        first_shop_months.cohort_month,
        inc_amount * 30 AS month_mrr

     FROM shop_days
     LEFT JOIN first_shop_months USING (shop_subdomain)
     WHERE dt IN (SELECT * FROM ends_of_months)
),

shop_month_activity AS (
    SELECT
        cohort_month,
        period_month,
        COUNT(DISTINCT shop_subdomain) AS retained_shops,
        SUM(month_mrr) AS retained_mrr
    FROM shop_months
    GROUP BY 1, 2
)

SELECT
    cohort_month,
    CONCAT(cohort_month, ' [', cohort_size, ' / ', to_varchar(cohort_starting_mrr, '$9,000'), ']') AS cohort_info,
    FLOOR(DATEDIFF(month, cohort_month, period_month)) as period,
    retained_shops,
    retained_shops / cohort_size::float AS retention_rate,
    retained_mrr,
    retained_mrr / cohort_starting_mrr AS revenue_retention_rate
FROM cohort_info

LEFT JOIN shop_month_activity USING (cohort_month)
WHERE
    period IS NOT NULL
ORDER BY 1, 2
