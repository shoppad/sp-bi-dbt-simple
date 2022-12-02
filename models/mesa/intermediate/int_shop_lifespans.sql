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

shop_dates AS (
    SELECT
        shop_subdomain,
        first_installed_at_pt::date AS first_dt,
        IFNULL(uninstalled_at_pt, {{ pacific_timestamp('CURRENT_TIMESTAMP()') }})::date AS last_dt
    FROM {{ ref('stg_shops') }}
),

custom_app_revenue AS (

    SELECT
        shop_subdomain,
        first_dt,
        COALESCE(last_dt, {{ pacific_timestamp('CURRENT_TIMESTAMP()') }}) AS last_dt
        {# TODO: Add start/end dates to custom apps. #}
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
    )
    GROUP BY 1
),

final AS (
    SELECT
        shop_subdomain,
        first_dt,
        last_dt,
        {{ datediff('first_dt', 'last_dt', 'day') }} + 1 AS lifespan_length
    FROM combined_dates
)

SELECT *
FROM final
