WITH RECURSIVE dates AS (
    -- start date
    SELECT '{{ var('start_date') }}'::DATE as dt
    UNION ALL
    SELECT DATEADD('day', 1, dt) as dt
    FROM dates
    -- end date (inclusive)
    WHERE dt <= current_date()
),

shops AS (
    SELECT
        shop_subdomain,
        first_installed_at
    FROM {{ ref('int_shops') }}
    WHERE install_status = 'active'
        AND shopify_plan_name NOT IN ('frozen', 'cancelled', 'fraudulent')
),

charges AS (
    SELECT
        shop_subdomain,
        charged_on_pt AS dt,
        subscription_id,
        billed_count,
        billed_amount AS daily_usage_revenue
    FROM {{ ref('mesa_charges') }}
    WHERE charged_on_pt < current_date()
),

daily_charges AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(billed_count), 0) AS billed_count,
        COALESCE(SUM(daily_usage_revenue), 0) AS daily_usage_revenue
    FROM dates
    INNER JOIN shops ON dt >= shops.first_installed_at
    LEFT JOIN charges USING (dt, shop_subdomain)
    GROUP BY 1, 2
),

billing_accounts AS (
    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}
    {# ?: Is it important to include this? Won't charges never appear if they are still in Trial?
        (
            billing_plan_trial_ends IS NULL OR
            billing_plan_trial_ends < current_date()
        ) #}

),

workflow_runs AS (
    SELECT
        shop_subdomain,
        workflow_run_on_pt AS dt,
        is_successful
    FROM {{ ref('workflow_runs') }}
),

daily_workflow_run_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(COUNT(*), 0) AS workflow_runs_count,
        COALESCE(COUNT_IF(is_successful), 0) AS workflow_runs_success_count,
        COALESCE((workflow_runs_success_count / NULLIF(workflow_runs_count, 0)), 0) AS workflow_success_percent
    FROM dates
    INNER JOIN shops ON dt >= shops.first_installed_at
    LEFT JOIN workflow_runs USING (shop_subdomain, dt)
    GROUP BY
        1,
        2
),

activity_dates AS (
    SELECT
        shop_subdomain,
        dt
    FROM daily_workflow_run_counts
    UNION ALL
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
        {{ dbt_utils.surrogate_key(['shop_subdomain','dt'] ) }} AS "{{ (model.name ~ '_id') | upper }}",
        (daily_plan_revenue + daily_usage_revenue) AS inc_amount
    FROM daily_workflow_run_counts
    FULL OUTER JOIN daily_charges USING (shop_subdomain, dt)
    INNER JOIN shops USING (shop_subdomain)
    LEFT JOIN daily_active_status USING (shop_subdomain, dt)
    LEFT JOIN billing_accounts USING (shop_subdomain)
    {# TODO: Create an "end date" with MAX() of charge, workflow run or uninstall. #}
)

SELECT * FROM final
