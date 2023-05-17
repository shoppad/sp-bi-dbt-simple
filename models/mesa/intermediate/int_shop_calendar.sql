WITH
shop_trial_end_dates AS (
    SELECT
        shop_subdomain,
        trial_end_dt AS dt,
        is_custom_app
    FROM {{ ref('stg_shops') }}
),

shop_plan_days AS (
    SELECT *
    FROM {{ ref('int_mesa_shop_plan_days') }}
),

shop_lifespans AS (
    SELECT *
    FROM {{ ref('int_shop_lifespans') }}
),

calendar_dates AS (
    SELECT date_day as dt
    FROM {{ ref('calendar_dates') }}
),

shop_calendar AS (
    SELECT
        shop_subdomain,
        dt,
        CASE
            WHEN (NOT(is_custom_app) AND shop_trial_end_dates.dt IS NOT NULL AND dt < shop_trial_end_dates.dt)
                THEN 0
            ELSE COALESCE(daily_plan_revenue, 0)
        END AS daily_plan_revenue,
        mesa_plan,
        shopify_plan
    FROM shop_lifespans
    INNER JOIN calendar_dates
        ON dt BETWEEN first_dt AND last_dt
    LEFT JOIN shop_plan_days USING (shop_subdomain, dt)
    LEFT JOIN shop_trial_end_dates USING (shop_subdomain)
)

SELECT *
FROM shop_calendar
