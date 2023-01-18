WITH
workflow_run_dates AS (
    SELECT
        shop_subdomain,
        MIN(workflow_run_on_pt) AS first_dt,
        MAX(workflow_run_on_pt) AS last_dt
    FROM {{ ref('int_workflow_runs') }}
    GROUP BY 1
),

charge_dates AS (
    SELECT
        shop_subdomain,
        MIN(charged_on_pt) AS first_dt,
        MAX(charged_on_pt) AS last_dt
    FROM {{ ref('stg_mesa_charges') }}
    GROUP BY 1
),

plan_dates AS (
    SELECT
        shop_subdomain,
        MIN(dt) AS first_dt,
        MAX(dt) AS last_dt
    FROM {{ ref('int_mesa_shop_plan_days') }}
    GROUP BY 1
),

shop_dates AS (
    SELECT
        shop_subdomain,
        first_installed_at_pt::DATE AS first_dt,
        CASE
            WHEN uninstalled_at_pt IS NULL OR status = 'active'
                THEN {{ pacific_timestamp('CURRENT_TIMESTAMP()') }}
            ELSE
                uninstalled_at_pt
        END::DATE AS last_dt
    FROM {{ ref('stg_shops') }}
),

custom_app_revenue AS (

    SELECT
        shop_subdomain,
        first_dt,
        last_dt
        {# TODO: Add start/end dates to custom apps seed file. #}
        {# ?: Some custom apps can't connect to real stores. This probably means some Workflows aren't being attributed to a Store either. #}
    FROM {{ ref('custom_app_daily_revenues') }}

),

combined_dates AS (
    SELECT
        shop_subdomain,
        MIN(first_dt) AS first_dt,
        MAX(last_dt) AS last_dt
    FROM (
        SELECT *
        FROM charge_dates
        UNION ALL
        SELECT *
        FROM workflow_run_dates
        UNION ALL
        SELECT *
        FROM shop_dates
        UNION ALL
        SELECT *
        FROM custom_app_revenue
        UNION ALL
        SELECT *
        FROM plan_dates
    )
    GROUP BY 1
),

final AS (
    SELECT
        shop_subdomain,
        combined_dates.first_dt::DATE AS first_dt,
        LEAST(
                COALESCE(combined_dates.last_dt, {{ pacific_timestamp('CURRENT_TIMESTAMP()') }}::DATE),
                COALESCE(shop_dates.last_dt, {{ pacific_timestamp('CURRENT_TIMESTAMP()') }}::DATE)
            ) AS last_dt,
        {{ datediff('first_dt', 'COALESCE(last_dt, ' ~ pacific_timestamp('CURRENT_TIMESTAMP()') ~ ')::DATE', 'day') }} + 1 AS lifespan_length
    FROM combined_dates
    LEFT JOIN shop_dates USING (shop_subdomain)-- Added to override in case of uninstall.
)

SELECT *
FROM final
