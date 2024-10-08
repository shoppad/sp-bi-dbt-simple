WITH
    shops AS (SELECT shop_subdomain, is_custom_app FROM {{ ref("stg_shops") }}),

    shop_plan_days AS (SELECT * FROM {{ ref("int_mesa_shop_plan_days") }}),

    shop_lifespans AS (SELECT * FROM {{ ref("int_shop_lifespans") }}),

    calendar_dates AS (
        SELECT date_day AS dt
        FROM {{ ref("calendar_dates") }}
        WHERE
            dt <= {{ pacific_timestamp("CURRENT_TIMESTAMP") }}::DATE - INTERVAL '1 DAY'
    ),

    shop_trial_days AS (
        SELECT shop_subdomain, dt, TRUE AS is_in_trial
        FROM calendar_dates
        INNER JOIN
            {{ ref("stg_trial_periods") }}
            ON calendar_dates.dt BETWEEN started_on_pt AND COALESCE(
                ended_on_pt, {{ pacific_timestamp("CURRENT_TIMESTAMP") }}::DATE - INTERVAL '1 DAY'
            )
    ),

    shop_calendar AS (
        SELECT
            shop_subdomain,
            dt,
            COALESCE(
                IFF(
                    is_shopify_zombie_plan OR COALESCE(is_in_trial, FALSE),
                    0,
                    daily_plan_revenue
                ),
                0
            ) AS daily_plan_revenue,
            mesa_plan,
            mesa_plan_identifier,
            COALESCE(shopify_plan, 'unavailable') AS shopify_plan,
            COALESCE(shop_trial_days.is_in_trial, FALSE) AS is_in_trial,
            COALESCE(is_shopify_zombie_plan, FALSE) AS is_shopify_zombie_plan

        FROM shop_lifespans
        INNER JOIN calendar_dates ON dt BETWEEN first_dt AND last_dt
        LEFT JOIN shop_plan_days USING (shop_subdomain, dt)
        LEFT JOIN shops USING (shop_subdomain)
        LEFT JOIN shop_trial_days USING (shop_subdomain, dt)
        ORDER BY dt
    )

SELECT *
FROM shop_calendar
