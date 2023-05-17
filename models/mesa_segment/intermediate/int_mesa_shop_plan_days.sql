WITH

calendar_dates AS (
    SELECT date_day AS dt
    FROM {{ ref('calendar_dates') }}
),

shop_trial_end_dts AS (
    SELECT
        shop_subdomain,
        trial_end_dt
    FROM {{ ref('stg_shops') }}
),

mesa_plan_changes AS (
    SELECT
        shop_subdomain,
        interval AS mesa_plan_interval,
        changed_on_pt AS dt,
        price AS mesa_plan_interval_price,
        plan AS mesa_plan,
        ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY changed_at_pt ASC) AS change_order
    FROM {{ ref('stg_mesa_plan_changes') }}
),

mesa_plan_calendar_dates AS (
    SELECT
        mesa_plan_changes.shop_subdomain,
        calendar_dates.dt,
        mesa_plan_changes.mesa_plan,
        mesa_plan_changes.mesa_plan_interval_price,
        mesa_plan_changes.mesa_plan_interval,
        CASE
            WHEN mesa_plan_changes.mesa_plan_interval = 'annual'
                THEN mesa_plan_changes.mesa_plan_interval_price / 365
            WHEN mesa_plan_changes.mesa_plan_interval = 'monthly'
                THEN mesa_plan_changes.mesa_plan_interval_price / DAY(LAST_DAY(calendar_dates.dt))
        END AS daily_plan_revenue
    FROM mesa_plan_changes
    LEFT JOIN mesa_plan_changes AS next_plan_changes
        ON mesa_plan_changes.shop_subdomain = next_plan_changes.shop_subdomain
            AND mesa_plan_changes.change_order + 1 = next_plan_changes.change_order
    INNER JOIN calendar_dates
        ON calendar_dates.dt BETWEEN mesa_plan_changes.dt AND COALESCE(next_plan_changes.dt - INTERVAL '1day', CURRENT_DATE)
),

{% set zombie_store_shopify_plans = ['frozen', 'fraudulent', 'paused', 'dormant', 'cancelled'] %}
shopify_plan_changes AS (
    SELECT
        shop_subdomain,
        changed_on_pt AS dt,
        plan IN ({{ "'" ~ zombie_store_shopify_plans | join("', '")  ~ "'" }}) AS is_zombie,
        plan AS shopify_plan,
        ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY changed_at_pt ASC) AS change_order
    FROM {{ ref('stg_shopify_plan_changes') }}
),

shopify_plan_calendar_dates AS (
    SELECT
        shopify_plan_changes.shop_subdomain,
        calendar_dates.dt,
        shopify_plan_changes.is_zombie,
        shopify_plan_changes.shopify_plan,
        shopify_plan_changes.change_order

    FROM shopify_plan_changes
    LEFT JOIN shopify_plan_changes AS next_shopify_plan_changes
        ON shopify_plan_changes.shop_subdomain = next_shopify_plan_changes.shop_subdomain
            AND shopify_plan_changes.change_order + 1 = next_shopify_plan_changes.change_order
    INNER JOIN calendar_dates
        ON calendar_dates.dt BETWEEN shopify_plan_changes.dt AND COALESCE(next_shopify_plan_changes.dt - INTERVAL '1day', CURRENT_DATE)
),

custom_app_daily_revenue AS (
    SELECT
        shop_subdomain,
        daily_plan_revenue AS custom_daily_plan_revenue,
        first_dt,
        last_dt
    FROM {{ ref('custom_app_daily_revenues') }}
),

custom_app_daily_revenue_dates AS (
    SELECT
        shop_subdomain,
        dt,
        custom_daily_plan_revenue,
        'custom-app' AS mesa_plan,
        custom_daily_plan_revenue AS mesa_plan_interval_price,
        'day' AS mesa_plan_interval
    FROM custom_app_daily_revenue
    INNER JOIN calendar_dates
        ON calendar_dates.dt
            BETWEEN custom_app_daily_revenue.first_dt
                AND COALESCE(custom_app_daily_revenue.last_dt, {{ pacific_timestamp('current_timestamp()') }}::DATE )
),

{# combined_revenue_dates AS (


    SELECT
        shop_subdomain,
        dt,
        daily_plan_revenue,
        mesa_plan,
        mesa_plan_interval_price,
        mesa_plan_interval
    FROM mesa_plan_calendar_dates
    UNION ALL
    SELECT
        shop_subdomain,
        dt,
        custom_daily_plan_revenue AS daily_plan_revenue,
        'custom-app' AS mesa_plan,
        custom_daily_plan_revenue AS mesa_plan_interval_price,
        'day' AS mesa_plan_interval
    FROM custom_app_daily_revenue_dates

), #}

final AS (
    SELECT
        dt,
        shop_subdomain,
        COALESCE(
            IFF(
                is_zombie OR (trial_end_dt IS NOT NULL AND dt <= trial_end_dt),
                0,
                daily_plan_revenue), 0) +
            COALESCE(custom_daily_plan_revenue, 0) AS daily_plan_revenue,
        COALESCE(mesa_plan_calendar_dates.mesa_plan, custom_app_daily_revenue_dates.mesa_plan) AS mesa_plan,
        shopify_plan,
        COALESCE(is_zombie, FALSE) AS is_shopify_zombie_plan
    FROM mesa_plan_calendar_dates
    FULL OUTER JOIN custom_app_daily_revenue_dates USING (shop_subdomain, dt)
    LEFT JOIN shop_trial_end_dts USING (shop_subdomain)
    LEFT JOIN shopify_plan_calendar_dates USING (shop_subdomain, dt)
)

SELECT *
FROM final
