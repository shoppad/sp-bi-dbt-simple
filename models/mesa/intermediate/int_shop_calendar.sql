WITH
daily_plan_revenues_non_unique AS (
    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('custom_app_daily_revenues') }}
    UNION ALL
    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}
),

summed_daily_plan_revenues AS (
    SELECT
        shop_subdomain,
        SUM(daily_plan_revenue) AS daily_plan_revenue
    FROM daily_plan_revenues_non_unique
    GROUP BY 1
),

shop_lifespans AS (
    SELECT *
    FROM {{ ref('int_shop_lifespans') }}
),

shop_calendar AS (
    SELECT
        shop_subdomain,
        date_day AS dt,
        COALESCE(daily_plan_revenue, 0) AS daily_plan_revenue
    FROM shop_lifespans
    INNER JOIN summed_daily_plan_revenues USING (shop_subdomain)
    INNER JOIN {{ ref('calendar_dates') }}
        ON dt BETWEEN first_dt AND last_dt
)

SELECT *
FROM shop_calendar
