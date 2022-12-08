WITH
plan_changes AS (
    SELECT
        * EXCLUDE (changed_on_pt),
        changed_on_pt AS dt,
        ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY changed_on_pt ASC) AS change_order
    FROM {{ ref('stg_plan_changes') }}
),

calendar_dates AS (
    SELECT date_day AS dt
    FROM {{ ref('calendar_dates') }}
),

custom_app_daily_revenue AS (
    SELECT
        shop_subdomain,
        daily_plan_revenue AS custom_daily_plan_revenue,
        first_dt,
        last_dt
    FROM {{ ref('custom_app_daily_revenues') }}

),

billing_calendar_dates AS (
    SELECT
        plan_changes.shop_subdomain,
        calendar_dates.dt,
        plan_changes.price AS plan_interval_price,
        plan_changes.interval,
        CASE
            WHEN plan_changes.interval = 'annual'
                THEN plan_changes.price / 365
            WHEN plan_changes.interval = 'monthly'
                THEN plan_changes.price / 30
        END AS daily_plan_revenue
    FROM plan_changes
    LEFT JOIN plan_changes AS next_plan_changes
        ON plan_changes.shop_subdomain = next_plan_changes.shop_subdomain
            AND plan_changes.change_order + 1 = next_plan_changes.change_order
    INNER JOIN calendar_dates
        ON calendar_dates.dt BETWEEN plan_changes.dt AND COALESCE(next_plan_changes.dt - INTERVAL '1day', CURRENT_DATE)
),

final AS (
    SELECT
        dt,
        billing_calendar_dates.shop_subdomain,
        CASE
            WHEN billing_calendar_dates.dt BETWEEN custom_app_daily_revenue.first_dt AND custom_app_daily_revenue.last_dt
                THEN daily_plan_revenue + custom_daily_plan_revenue
            ELSE daily_plan_revenue
        END AS daily_plan_revenue
    FROM billing_calendar_dates
    LEFT JOIN custom_app_daily_revenue USING (shop_subdomain)
)

SELECT *
FROM final
