WITH
daily_plan_revenues AS (

    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('custom_app_daily_revenues') }}
    UNION
    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY daily_plan_revenue DESC) = 1

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
    INNER JOIN daily_plan_revenues USING (shop_subdomain)
    INNER JOIN {{ ref('calendar_dates') }}
        ON dt BETWEEN first_dt AND last_dt
)

SELECT *
FROM shop_calendar
