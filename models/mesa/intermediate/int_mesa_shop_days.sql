WITH

shop_calendar AS (
    SELECT *
    FROM {{ ref('int_shop_calendar') }}
),

first_shop_calendar_days AS (
    SELECT
        shop_subdomain,
        MIN(dt) AS first_calendar_dt
    FROM shop_calendar
    GROUP BY 1
),

shop_cohort_dates AS (
    SELECT
        shop_subdomain,
        COALESCE(cohort_week, DATE_TRUNC('week', first_calendar_dt)::DATE) AS cohort_week,
        COALESCE(cohort_month, DATE_TRUNC('month', first_calendar_dt)::DATE) AS cohort_month
    FROM {{ ref('stg_shops') }}
    FULL OUTER JOIN first_shop_calendar_days
        USING (shop_subdomain)
),

workflow_runs AS (
    SELECT
        shop_subdomain,
        workflow_run_id,
        workflow_run_on_pt AS dt,
        is_successful,
        is_failure,
        is_stop
    FROM {{ ref('int_workflow_runs') }}
    WHERE workflow_name NOT ILIKE '%report card%'
),

daily_workflow_run_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(COUNT(workflow_runs.workflow_run_id), 0) AS workflow_runs_attempted_count,
        COALESCE(COUNT_IF(workflow_runs.is_successful), 0) AS workflow_runs_success_count,
        COALESCE(COUNT_IF(workflow_runs.is_failure), 0) AS workflow_runs_failure_count,
        COALESCE(COUNT_IF(workflow_runs.is_stop), 0) AS workflow_runs_stop_count,
        COALESCE((workflow_runs_success_count / NULLIF(workflow_runs_attempted_count, 0)), 0) AS workflow_success_percent
    FROM shop_calendar
    LEFT JOIN workflow_runs USING (shop_subdomain, dt)
    GROUP BY
        1,
        2
),

daily_step_counts AS (
    SELECT *
    FROM {{ ref('int_successful_step_run_day_counts') }}
),

thirty_day_workflow_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(workflow_runs_attempted_count), 0) AS workflow_run_attempt_rolling_thirty_day_count,
        COALESCE(SUM(workflow_runs_success_count), 0) AS workflow_run_success_rolling_thirty_day_count,
        COALESCE(SUM(workflow_runs_failure_count), 0) AS workflow_run_failure_rolling_thirty_day_count,
        COALESCE(SUM(workflow_runs_stop_count), 0) AS workflow_run_stop_rolling_thirty_day_count
    FROM shop_calendar
    LEFT JOIN daily_workflow_run_counts USING (shop_subdomain)
    WHERE
        daily_workflow_run_counts.dt BETWEEN DATEADD(DAY, -30, shop_calendar.dt) AND shop_calendar.dt
    GROUP BY 1, 2
),

thirty_day_step_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(total_workflow_steps_count), 0) AS total_workflow_steps_rolling_thirty_day_count,
        COALESCE(SUM(input_step_count), 0) AS input_step_rolling_thirty_day_count,
        COALESCE(SUM(output_step_count), 0) AS output_step_rolling_thirty_day_count
    FROM shop_calendar
    LEFT JOIN daily_step_counts USING (shop_subdomain)
    WHERE daily_step_counts.dt BETWEEN DATEADD(DAY, -30, shop_calendar.dt) AND shop_calendar.dt
    GROUP BY 1, 2
),

year_workflow_counts AS (
    SELECT
        shop_subdomain,
        dt,
        SUM(COALESCE(workflow_runs_attempted_count, 0)) AS workflow_run_attempt_rolling_year_count,
        SUM(COALESCE(workflow_runs_success_count, 0)) AS workflow_run_success_rolling_year_count,
        SUM(COALESCE(workflow_runs_failure_count, 0)) AS workflow_run_failure_rolling_year_count,
        SUM(COALESCE(workflow_runs_stop_count, 0)) AS workflow_run_stop_rolling_year_count
    FROM shop_calendar
    LEFT JOIN daily_workflow_run_counts USING (shop_subdomain)
    WHERE daily_workflow_run_counts.dt BETWEEN DATEADD(YEAR, -1, shop_calendar.dt) AND shop_calendar.dt
    GROUP BY 1, 2
),

charges AS (
    SELECT
        shop_subdomain,
        billed_count,
        billed_amount,
        charged_on_pt AS dt
    FROM {{ ref('stg_mesa_charges') }}
),

legacy_daus AS (
    SELECT
        shop_subdomain,
        dt,
        daily_usage_revenue
    FROM {{ ref('stg_legacy_daus') }}
),

daily_charges AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(charges.billed_count), 0) AS billed_count,
        COALESCE(SUM(IFF(is_in_trial, 0, COALESCE(charges.billed_amount, legacy_daus.daily_usage_revenue))), 0) AS daily_usage_revenue
    FROM shop_calendar
    LEFT JOIN charges USING (dt, shop_subdomain)
    LEFT JOIN legacy_daus USING (dt, shop_subdomain)
    GROUP BY 1, 2
),

thirty_day_revenue_totals AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(daily_plan_revenue + daily_usage_revenue), 0) AS income_rolling_thirty_day_total
    FROM shop_calendar
    LEFT JOIN daily_charges USING (shop_subdomain)
    WHERE daily_charges.dt BETWEEN DATEADD(DAY, -30, shop_calendar.dt) AND shop_calendar.dt
    GROUP BY 1, 2
),

year_revenue_totals AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(SUM(daily_plan_revenue + daily_usage_revenue), 0) AS income_rolling_year_total
    FROM shop_calendar
    LEFT JOIN daily_charges USING (shop_subdomain)
    WHERE daily_charges.dt BETWEEN DATEADD(YEAR, -1, shop_calendar.dt) AND shop_calendar.dt
    GROUP BY 1, 2
),

final AS (
    SELECT
        *,
        {{- dbt_utils.generate_surrogate_key(['shop_subdomain','dt'] ) }} AS mesa_shop_days_id,
        IFF(is_in_trial, 0, daily_plan_revenue + daily_usage_revenue) AS inc_amount,
        workflow_run_success_rolling_thirty_day_count >= {{ var('activation_workflow_run_count') }} AS is_active
    FROM shop_calendar
    LEFT JOIN daily_workflow_run_counts USING (shop_subdomain, dt)
    LEFT JOIN daily_charges USING (shop_subdomain, dt)
    LEFT JOIN thirty_day_workflow_counts USING (shop_subdomain, dt)
    LEFT JOIN year_workflow_counts USING (shop_subdomain, dt)
    LEFT JOIN shop_cohort_dates USING (shop_subdomain)
    LEFT JOIN thirty_day_revenue_totals USING (shop_subdomain, dt)
    LEFT JOIN year_revenue_totals USING (shop_subdomain, dt)
    LEFT JOIN daily_step_counts USING (shop_subdomain, dt)
    LEFT JOIN thirty_day_step_counts USING (shop_subdomain, dt)
)

SELECT *
FROM final
ORDER BY shop_subdomain, dt
