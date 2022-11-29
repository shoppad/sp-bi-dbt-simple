WITH
billing_accounts AS (

    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}

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
    INNER JOIN billing_accounts USING (shop_subdomain)
    INNER JOIN {{ ref('calendar_dates') }}
        ON dt BETWEEN first_dt AND last_dt
)

SELECT * FROM shop_calendar
