WITH
shops AS (
    SELECT
        shop_subdomain,
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
    SELECT date_day AS dt
    FROM {{ ref('calendar_dates') }}
),

shop_calendar AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(daily_plan_revenue, 0) AS daily_plan_revenue,
        mesa_plan,
        mesa_plan_identifier,
        COALESCE(shopify_plan, 'unavailable') AS shopify_plan,
        is_in_trial,
        is_shopify_zombie_plan
    FROM shop_lifespans
    INNER JOIN calendar_dates
        ON dt BETWEEN first_dt AND last_dt
    LEFT JOIN shop_plan_days USING (shop_subdomain, dt)
    LEFT JOIN shops USING (shop_subdomain)
)

SELECT *
FROM shop_calendar
