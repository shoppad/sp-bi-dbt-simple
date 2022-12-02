WITH
shop_cohort_dates AS (
    SELECT
        shop_subdomain,
        cohort_week,
        cohort_month
    FROM {{ ref('stg_shops') }}
),

shop_calendar AS (
    SELECT *
    FROM {{ ref('int_shop_calendar') }}
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
    FROM shop_calendar
    LEFT JOIN workflow_runs USING (shop_subdomain, dt)
    GROUP BY
        1,
        2
),

charges AS (
    SELECT
        shop_subdomain,
        billed_count,
        billed_amount,
        charged_on_pt AS dt
    FROM {{ ref('stg_mesa_charges') }}
),

daily_charges AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(billed_count), 0) AS billed_count,
        COALESCE(SUM(billed_amount), 0) AS daily_usage_revenue
    FROM shop_calendar
    LEFT JOIN charges USING (dt, shop_subdomain)
    GROUP BY 1, 2
),

thirty_day_workflow_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(workflow_runs_success_count), 0) AS workflow_runs_rolling_thirty_day_count
    FROM shop_calendar
    LEFT JOIN daily_workflow_run_counts USING (shop_subdomain)
    WHERE dt BETWEEN DATEADD(day, -30, dt) AND dt
    GROUP BY 1, 2
),

year_workflow_counts AS (
    SELECT
        shop_subdomain,
        dt,
        SUM(COALESCE(workflow_runs_success_count, 0)) AS workflow_runs_rolling_year_count
    FROM shop_calendar
    LEFT JOIN daily_workflow_run_counts USING (shop_subdomain)
    WHERE dt BETWEEN DATEADD(day, -365, dt) AND dt
    GROUP BY 1, 2
),

final AS (
    SELECT
        *,
        {{ dbt_utils.surrogate_key(['shop_subdomain','dt'] ) }} AS mesa_shop_days_id,
        daily_plan_revenue + daily_usage_revenue AS inc_amount,
        inc_amount > 0 OR workflow_runs_rolling_thirty_day_count >= {{ var('activation_workflow_run_count') }} AS is_active
    FROM shop_calendar
    LEFT JOIN daily_workflow_run_counts USING (shop_subdomain, dt)
    LEFT JOIN daily_charges USING (shop_subdomain, dt)
    LEFT JOIN thirty_day_workflow_counts USING (shop_subdomain, dt)
    LEFT JOIN year_workflow_counts USING (shop_subdomain, dt)
    LEFT JOIN shop_cohort_dates USING (shop_subdomain)
)

SELECT * FROM final
