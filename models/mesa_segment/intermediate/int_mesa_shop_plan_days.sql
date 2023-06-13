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
        planid AS mesa_plan_identifier,
        ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY changed_at_pt ASC) AS change_order
    FROM {{ ref('stg_mesa_plan_changes') }}
),

mesa_plan_calendar_dates AS (
    SELECT
        mesa_plan_changes.shop_subdomain,
        calendar_dates.dt,
        mesa_plan_changes.mesa_plan,
        mesa_plan_changes.mesa_plan_identifier,
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
        ON
            mesa_plan_changes.shop_subdomain = next_plan_changes.shop_subdomain
            AND mesa_plan_changes.change_order + 1 = next_plan_changes.change_order
    INNER JOIN calendar_dates
        ON calendar_dates.dt BETWEEN mesa_plan_changes.dt AND COALESCE(next_plan_changes.dt - INTERVAL '1day', CURRENT_DATE)
),

shopify_plan_changes AS (
    SELECT
        shop_subdomain,
        changed_on_pt AS dt,
        plan AS shopify_plan,
        oldplan AS old_shopify_plan,
        COALESCE(plan IN ({{ "'" ~ var('zombie_store_shopify_plans') | join("', '")  ~ "'" }}), FALSE) AS is_zombie,
        ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY changed_at_pt ASC) AS change_order
    FROM {{ ref('stg_shopify_plan_changes') }}
),

initial_shopify_plan_simulations AS (
    SELECT
        stg_shops.shop_subdomain,
        first_installed_on_pt AS dt,
        COALESCE(shopify_plan_changes.old_shopify_plan, analytics:initial:shopify_plan_name) AS shopify_plan,
        NULL AS old_shopify_plan,
        COALESCE(old_shopify_plan IN ({{ "'" ~ zombie_store_shopify_plans | join("', '")  ~ "'" }}), FALSE) AS is_zombie,
        0 AS change_order
    FROM {{ ref("stg_shops") }}
    LEFT JOIN
        shopify_plan_changes
        ON
            stg_shops.shop_subdomain = shopify_plan_changes.shop_subdomain
            AND change_order = 1
),

shopify_plan_calendar_dates AS (
    SELECT
        combined_shopify_plan_changes.* EXCLUDE (dt, old_shopify_plan, change_order),
        calendar_dates.dt
    FROM
        (
            SELECT *
            FROM shopify_plan_changes

            UNION ALL

            SELECT *
            FROM initial_shopify_plan_simulations
        ) AS combined_shopify_plan_changes
    LEFT JOIN shopify_plan_changes AS next_shopify_plan_changes
        ON
            combined_shopify_plan_changes.shop_subdomain = next_shopify_plan_changes.shop_subdomain
            AND combined_shopify_plan_changes.change_order + 1 = next_shopify_plan_changes.change_order
    INNER JOIN calendar_dates
        ON calendar_dates.dt BETWEEN combined_shopify_plan_changes.dt AND COALESCE(next_shopify_plan_changes.dt - INTERVAL '1day', CURRENT_DATE)
    ORDER BY combined_shopify_plan_changes.change_order ASC
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
        'custom-app' AS mesa_plan_identifier,
        custom_daily_plan_revenue AS mesa_plan_interval_price,
        'day' AS mesa_plan_interval
    FROM custom_app_daily_revenue
    INNER JOIN calendar_dates
        ON
            calendar_dates.dt
            BETWEEN custom_app_daily_revenue.first_dt
            AND COALESCE(custom_app_daily_revenue.last_dt, {{ pacific_timestamp('current_timestamp()') }}::DATE)
),

final AS (
    SELECT
        dt,
        shop_subdomain,
        (trial_end_dt IS NOT NULL AND dt <= trial_end_dt) AS is_in_trial,
        COALESCE(
            IFF(
                is_zombie OR is_in_trial,
                0,
                daily_plan_revenue
            ), 0
        )
        + COALESCE(custom_daily_plan_revenue, 0) AS daily_plan_revenue,
        COALESCE(mesa_plan_calendar_dates.mesa_plan, custom_app_daily_revenue_dates.mesa_plan) AS mesa_plan,
        COALESCE(mesa_plan_calendar_dates.mesa_plan_identifier, custom_app_daily_revenue_dates.mesa_plan_identifier) AS mesa_plan_identifier,
        shopify_plan,
        is_zombie,
        COALESCE(is_zombie, FALSE) AS is_shopify_zombie_plan
    FROM mesa_plan_calendar_dates
    FULL OUTER JOIN custom_app_daily_revenue_dates USING (shop_subdomain, dt)
    LEFT JOIN shop_trial_end_dts USING (shop_subdomain)
    LEFT JOIN shopify_plan_calendar_dates USING (shop_subdomain, dt)
)

SELECT *
FROM final
