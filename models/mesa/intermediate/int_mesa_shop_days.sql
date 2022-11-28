WITH dates AS (
    SELECT
        date_day AS dt
    FROM {{ ref('calendar_dates') }}
),

shops AS (
    SELECT
        shop_subdomain,
        first_installed_at_pt
    FROM {{ ref('stg_shops') }}
),

workflow_runs AS (
    SELECT
        shop_subdomain,
        workflow_run_on_pt AS dt,
        is_successful
    FROM {{ ref('int_workflow_runs') }}
),

daily_workflow_run_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(COUNT(workflow_runs.*), 0) AS workflow_runs_count,
        COALESCE(COUNT_IF(workflow_runs.is_successful), 0) AS workflow_runs_success_count,
        COALESCE((workflow_runs_success_count / NULLIF(workflow_runs_count, 0)), 0) AS workflow_success_percent
    FROM dates
    INNER JOIN shops ON dt >= shops.first_installed_at_pt
    LEFT JOIN workflow_runs USING (shop_subdomain, dt)
    GROUP BY
        1,
        2
),

charges AS (
    SELECT
        shop_subdomain,
        charged_on_pt AS dt,
        subscription_id,
        billed_count,
        billed_amount AS daily_usage_revenue
    FROM {{ ref('stg_mesa_charges') }}
    WHERE charged_on_pt < current_date()
),


daily_charges AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(billed_count), 0) AS billed_count,
        COALESCE(SUM(daily_usage_revenue), 0) AS daily_usage_revenue
    FROM dates
    INNER JOIN shops ON dt >= shops.first_installed_at_pt
    LEFT JOIN charges USING (dt, shop_subdomain)
    GROUP BY 1, 2
),

activity_dates AS (
    SELECT
        shop_subdomain,
        dt
    FROM daily_workflow_run_counts
    UNION
    SELECT
        shop_subdomain,
        dt
    FROM daily_charges
),

daily_active_status AS (
    SELECT
        daily_workflow_run_counts.shop_subdomain,
        daily_workflow_run_counts.dt,
        COALESCE(SUM(window_days.workflow_runs_success_count), 0) AS workflow_runs_rolling_thirty_day_count,
        workflow_runs_rolling_thirty_day_count >= {{ var('activation_workflow_run_count') }} AS is_active
    FROM activity_dates
    FULL OUTER JOIN daily_workflow_run_counts USING (shop_subdomain, dt)
    INNER JOIN daily_workflow_run_counts AS window_days
        ON daily_workflow_run_counts.shop_subdomain = window_days.shop_subdomain AND daily_workflow_run_counts.dt BETWEEN window_days.dt - 30 AND window_days.dt
    GROUP BY
        1,
        2
),

final AS (
    SELECT
        *,
        {{ dbt_utils.surrogate_key(['shop_subdomain','dt'] ) }} AS mesa_shop_days_id
    FROM daily_workflow_run_counts
    FULL OUTER JOIN daily_charges USING (shop_subdomain, dt)
    INNER JOIN shops USING (shop_subdomain)
    LEFT JOIN daily_active_status USING (shop_subdomain, dt)
    {# TODO: Create an "end date" with MAX() of charge, workflow run or uninstall. #}
)

SELECT * FROM final
