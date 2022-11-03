WITH RECURSIVE dates AS (
    -- start date
    SELECT '{{ var('start_date') }}'::DATE as dt
    UNION ALL
    SELECT DATEADD('day',1,dt) as dt
    FROM dates
    -- end date (inclusive)
    WHERE dt <= current_date()
),

charges AS (
    SELECT
        shop_id,
        charged_on_pt AS dt,
        subscription_id,
        billed_count,
        billed_amount AS daily_usage_revenue
    FROM {{ ref('mesa_charges') }}
    WHERE charged_on_pt < current_date()
),

charges_by_day AS (
    SELECT
        charges.*
    FROM dates
    INNER JOIN shops ON dt >= shops.first_installed_at
    LEFT JOIN charges USING (dt, shop_id)
),

billing_accounts AS (
    SELECT
        shop_id,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}
    {# Is it important to include this? Won't charges never appear if they are still in Trial?
        (
            billing_plan_trial_ends IS NULL OR
            billing_plan_trial_ends < current_date()
        ) #}

),

shops AS (
    SELECT
        shop_id,
        shop_subdomain,
        first_installed_at
    FROM {{ ref('stg_shops') }}
    WHERE install_status = 'active'
        AND shopify_plan_name NOT IN ('frozen', 'cancelled', 'fraudulent')
),

workflow_runs AS (
    SELECT
        shop_id,
        run_on_pt AS dt,
        is_successful
    FROM {{ ref('workflow_runs') }}
),

daily_workflow_run_counts AS (
    SELECT
        shop_id,
        dt,
        COALESCE(COUNT(*), 0) AS trigger_runs_count,
        COUNT_IF(is_successful) AS successful_workflow_runs_count,
        (successful_workflow_runs_count / NULLIF(trigger_runs_count, 0)) AS workflow_success_percent
    FROM dates
    INNER JOIN shops ON dt >= shops.first_installed_at
    INNER JOIN workflow_runs USING (shop_id, dt)
    GROUP BY
        1,
        2
),

daily_active_status AS (
    SELECT
        daily_workflow_run_counts.shop_id,
        daily_workflow_run_counts.dt,
        daily_workflow_run_counts.successful_workflow_runs_count >= {{ var('activation_workflow_run_count') }} AS is_active,
        SUM(window_days.successful_workflow_runs_count) AS rolling_thirty_day_workflow_count
    FROM daily_workflow_run_counts
    INNER JOIN daily_workflow_run_counts as window_days
        ON daily_workflow_run_counts.shop_id = window_days.shop_id AND daily_workflow_run_counts.dt BETWEEN window_days.dt - 30 AND window_days.dt
    GROUP BY
        1,
        2,
        3
),

final AS (
    SELECT
        *,
        (daily_plan_revenue + daily_usage_revenue) as inc_amount
    FROM daily_workflow_run_counts
    FULL OUTER JOIN charges_by_day USING (shop_id, dt)
    INNER JOIN shops USING (shop_id)
    LEFT JOIN daily_active_status USING (shop_id, dt)
    LEFT JOIN billing_accounts USING (shop_id)

    -- Don't create rows for zero amounts.
    {# WHERE inc_amount > 0 -- This is handled in the Growth Accounting queries. #}
)
SELECT * FROM final
